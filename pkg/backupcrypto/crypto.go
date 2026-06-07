// Package backupcrypto wraps a BackupTarget Push/Pull stream in
// authenticated encryption. The cipher is AEAD (ChaCha20-Poly1305 by
// default, AES-256-GCM as an alternate for AESNI hosts) ; the key is
// derived from an operator-supplied passphrase via Argon2id.
//
// The format is intentionally simple :
//
//   [stream header : 32 bytes magic+version+algorithm-id+chunk-size]
//   [chunk 0  : 12-byte nonce || ciphertext || 16-byte tag]
//   [chunk 1  : 12-byte nonce || ciphertext || 16-byte tag]
//   ...
//
// Per-chunk nonces start from a random 8-byte stream-prefix + a 4-byte
// monotonic counter. The prefix is random per backup ; the counter
// monotonically increases. This keeps each (key, nonce) pair unique
// across an entire backup (counter rolls at 2^32 chunks = ≈ 17 PiB at
// 4 MiB chunks — well above any practical block volume size).
//
// AAD : the stream header. Tampering with chunk size or algorithm-id
// invalidates every chunk's tag.
//
// The salt + algorithm-id + chunk-size live in the backup's metadata
// (driver.go's sidecar JSON for fs/s3/sftp, manifest annotations for
// OCI) so restore can re-derive the key and parse the stream.

package backupcrypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/chacha20poly1305"
)

// Algorithm identifiers used in the on-wire header AND in the
// drivers.BackupEncryption{Info}.Algorithm field. Plain strings so they
// survive serialisation through any wire format we plug in later.
const (
	AlgChaCha20Poly1305 = "chacha20-poly1305"
	AlgAES256GCM        = "aes-256-gcm"

	KDFArgon2id = "argon2id"
	KDFRaw      = "raw" // env value is a hex-encoded 256-bit key (KMS-managed)

	defaultChunkSize     = 4 * 1024 * 1024 // 4 MiB
	nonceLen             = 12
	tagLen               = 16
	streamHeaderLen      = 32
	streamHeaderMagicLen = 8
	streamPrefixLen      = 8 // first 8 bytes of the per-chunk nonce
)

var streamHeaderMagic = [streamHeaderMagicLen]byte{'W', 'E', 'F', 'T', 'B', 'A', 'K', '1'}

// Default Argon2id params. OWASP's "draft m=64 MiB, t=3, p=2" — strong
// enough for backup-grade passphrases, fast enough not to gate the CLI
// for more than ~200 ms on a modern CPU.
const (
	defaultArgon2Memory      = 64 * 1024 // KiB
	defaultArgon2Iterations  = 3
	defaultArgon2Parallelism = 2
	derivedKeyLen            = 32 // ChaCha20 + AES-256 both want 256-bit keys
	saltLen                  = 16
)

// Params is the full crypto spec needed to encrypt or decrypt one
// backup. Created by ResolveParams from the driver-level
// BackupEncryption / BackupEncryptionInfo + the per-backup salt.
type Params struct {
	Algorithm string // AlgChaCha20Poly1305 | AlgAES256GCM
	Key       []byte // derived key (32 bytes)
	ChunkSize int    // ciphertext chunk size used by the encoder
}

// DeriveKey turns a passphrase + salt into a 32-byte key using the chosen
// KDF + parameters. kdfParams map is mutation-free (caller-supplied
// values picked up via key lookup ; missing keys = sensible defaults).
func DeriveKey(passphrase, salt []byte, kdf string, kdfParams map[string]string) ([]byte, error) {
	if len(passphrase) == 0 {
		return nil, errors.New("backupcrypto: empty passphrase")
	}
	switch kdf {
	case "", KDFArgon2id:
		mem := lookupUint(kdfParams, "memory_kib", defaultArgon2Memory)
		iter := lookupUint(kdfParams, "iterations", defaultArgon2Iterations)
		par := lookupUint(kdfParams, "parallelism", defaultArgon2Parallelism)
		return argon2.IDKey(passphrase, salt, uint32(iter), uint32(mem), uint8(par), derivedKeyLen), nil
	case KDFRaw:
		// passphrase IS the hex-encoded key — KMS-managed deployments.
		raw, err := hex.DecodeString(string(passphrase))
		if err != nil {
			return nil, fmt.Errorf("backupcrypto: kdf=raw expects hex-encoded passphrase: %w", err)
		}
		if len(raw) != derivedKeyLen {
			return nil, fmt.Errorf("backupcrypto: kdf=raw key must be %d bytes (got %d)", derivedKeyLen, len(raw))
		}
		return raw, nil
	default:
		return nil, fmt.Errorf("backupcrypto: unknown kdf %q (want %q or %q)", kdf, KDFArgon2id, KDFRaw)
	}
}

// LoadPassphrase reads the passphrase from the configured env var. Empty
// or unset env is an error — we DO NOT fall back to prompting (this code
// runs unattended in agents / cron jobs ; prompts there are stuck
// processes nobody notices).
func LoadPassphrase(envName string) ([]byte, error) {
	if envName == "" {
		return nil, errors.New("backupcrypto: encryption configured but passphrase_env is empty")
	}
	v := os.Getenv(envName)
	if v == "" {
		return nil, fmt.Errorf("backupcrypto: env %q is unset or empty", envName)
	}
	return []byte(v), nil
}

// NewSalt returns saltLen cryptographically-random bytes.
func NewSalt() ([]byte, error) {
	b := make([]byte, saltLen)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	return b, nil
}

// newAEAD builds the cipher.AEAD for an algorithm. Single-stop so callers
// (encoder / decoder) don't duplicate the algorithm string match.
func newAEAD(algorithm string, key []byte) (cipher.AEAD, error) {
	switch algorithm {
	case "", AlgChaCha20Poly1305:
		return chacha20poly1305.New(key)
	case AlgAES256GCM:
		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, err
		}
		return cipher.NewGCM(block)
	default:
		return nil, fmt.Errorf("backupcrypto: unknown algorithm %q", algorithm)
	}
}

// Encoder wraps an io.Writer to apply chunk-AEAD on every Write call.
// Caller must Close() to flush the final chunk + sync internal state.
type Encoder struct {
	dst       io.Writer
	aead      cipher.AEAD
	prefix    [streamPrefixLen]byte // random per stream
	counter   uint32                // chunk index
	chunkSize int
	header    []byte // stream header used as AAD
	headSent  bool
	buf       []byte
	closed    bool
}

// NewEncoder constructs an encoder. Writes shorter than ChunkSize are
// buffered until the chunk is full ; the final Close() ships whatever's
// in the buffer as a short final chunk.
func NewEncoder(dst io.Writer, p Params) (*Encoder, error) {
	if p.ChunkSize <= 0 {
		p.ChunkSize = defaultChunkSize
	}
	aead, err := newAEAD(p.Algorithm, p.Key)
	if err != nil {
		return nil, err
	}
	enc := &Encoder{
		dst:       dst,
		aead:      aead,
		chunkSize: p.ChunkSize,
		buf:       make([]byte, 0, p.ChunkSize),
	}
	if _, err := rand.Read(enc.prefix[:]); err != nil {
		return nil, err
	}
	enc.header = enc.buildHeader(p.Algorithm)
	return enc, nil
}

func (e *Encoder) buildHeader(algorithm string) []byte {
	h := make([]byte, streamHeaderLen)
	copy(h[0:8], streamHeaderMagic[:])
	h[8] = 1 // version
	switch algorithm {
	case "", AlgChaCha20Poly1305:
		h[9] = 1
	case AlgAES256GCM:
		h[9] = 2
	}
	binary.BigEndian.PutUint32(h[10:14], uint32(e.chunkSize))
	copy(h[14:14+streamPrefixLen], e.prefix[:])
	// Bytes 22..31 are zero / reserved for forward-compatible flags.
	return h
}

// Write absorbs bytes into the current chunk ; flushes whenever a chunk
// is full. Returns the number of plaintext bytes accepted (always
// len(p) on success — partial-accept isn't useful here).
func (e *Encoder) Write(p []byte) (int, error) {
	if e.closed {
		return 0, errors.New("backupcrypto: write after close")
	}
	if !e.headSent {
		if _, err := e.dst.Write(e.header); err != nil {
			return 0, fmt.Errorf("write stream header: %w", err)
		}
		e.headSent = true
	}
	written := 0
	for len(p) > 0 {
		need := e.chunkSize - len(e.buf)
		if need > len(p) {
			need = len(p)
		}
		e.buf = append(e.buf, p[:need]...)
		p = p[need:]
		written += need
		if len(e.buf) == e.chunkSize {
			if err := e.flushChunk(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

func (e *Encoder) flushChunk() error {
	nonce := e.nonceForChunk(e.counter)
	out := e.aead.Seal(nil, nonce, e.buf, e.header)
	if _, err := e.dst.Write(nonce); err != nil {
		return fmt.Errorf("write nonce: %w", err)
	}
	if _, err := e.dst.Write(out); err != nil {
		return fmt.Errorf("write ciphertext: %w", err)
	}
	e.buf = e.buf[:0]
	e.counter++
	return nil
}

func (e *Encoder) nonceForChunk(counter uint32) []byte {
	n := make([]byte, nonceLen)
	copy(n[0:streamPrefixLen], e.prefix[:])
	binary.BigEndian.PutUint32(n[streamPrefixLen:streamPrefixLen+4], counter)
	return n
}

// Close flushes whatever's buffered + writes the final chunk. After
// Close, the encoder is unusable.
func (e *Encoder) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	if !e.headSent {
		// Empty stream still emits the header so decoder sees a valid
		// magic + version even for zero-byte backups (defensive).
		if _, err := e.dst.Write(e.header); err != nil {
			return fmt.Errorf("write header on close: %w", err)
		}
	}
	if len(e.buf) > 0 {
		if err := e.flushChunk(); err != nil {
			return err
		}
	}
	return nil
}

// Decoder wraps an io.Reader of an encoder-produced stream, surfacing
// plaintext bytes via Read. Header parsing happens on the first Read ;
// each subsequent chunk Open call rebuilds the nonce from the prefix +
// counter and authenticates against the header AAD.
type Decoder struct {
	src       io.Reader
	aead      cipher.AEAD
	algorithm string
	chunkSize int
	prefix    [streamPrefixLen]byte
	counter   uint32
	header    []byte
	headRead  bool
	pending   []byte // decrypted bytes from a partial chunk read
	done      bool
}

// NewDecoder constructs a decoder. The expected algorithm + key must
// match the encoder side ; mismatched key means every chunk fails to
// authenticate (Read returns an error immediately on the first chunk).
func NewDecoder(src io.Reader, p Params) (*Decoder, error) {
	aead, err := newAEAD(p.Algorithm, p.Key)
	if err != nil {
		return nil, err
	}
	if p.ChunkSize <= 0 {
		p.ChunkSize = defaultChunkSize
	}
	return &Decoder{
		src:       src,
		aead:      aead,
		algorithm: p.Algorithm,
		chunkSize: p.ChunkSize,
	}, nil
}

// readHeader pulls the 32-byte header, validates magic + version, and
// stashes the chunk-size + per-stream prefix.
func (d *Decoder) readHeader() error {
	hdr := make([]byte, streamHeaderLen)
	if _, err := io.ReadFull(d.src, hdr); err != nil {
		return fmt.Errorf("read stream header: %w", err)
	}
	if string(hdr[0:streamHeaderMagicLen]) != string(streamHeaderMagic[:]) {
		return errors.New("backupcrypto: stream magic mismatch (not a weft-block encrypted backup)")
	}
	if hdr[8] != 1 {
		return fmt.Errorf("backupcrypto: stream version %d unsupported", hdr[8])
	}
	algoByte := hdr[9]
	switch algoByte {
	case 1:
		if d.algorithm != "" && d.algorithm != AlgChaCha20Poly1305 {
			return fmt.Errorf("backupcrypto: header says chacha20-poly1305, decoder configured for %q", d.algorithm)
		}
	case 2:
		if d.algorithm != "" && d.algorithm != AlgAES256GCM {
			return fmt.Errorf("backupcrypto: header says aes-256-gcm, decoder configured for %q", d.algorithm)
		}
	default:
		return fmt.Errorf("backupcrypto: unknown algorithm byte %d", algoByte)
	}
	d.chunkSize = int(binary.BigEndian.Uint32(hdr[10:14]))
	copy(d.prefix[:], hdr[14:14+streamPrefixLen])
	d.header = hdr
	d.headRead = true
	return nil
}

// Read pulls plaintext bytes. Caller's buffer is filled from any pending
// decrypted bytes first, then by reading + decrypting the next chunk.
func (d *Decoder) Read(p []byte) (int, error) {
	if d.done && len(d.pending) == 0 {
		return 0, io.EOF
	}
	if !d.headRead {
		if err := d.readHeader(); err != nil {
			return 0, err
		}
	}
	if len(d.pending) > 0 {
		n := copy(p, d.pending)
		d.pending = d.pending[n:]
		return n, nil
	}
	// Read next chunk : nonce + (chunkSize + tagLen) ciphertext for full
	// chunks, smaller for the final short one.
	nonce := make([]byte, nonceLen)
	if _, err := io.ReadFull(d.src, nonce); err != nil {
		if err == io.EOF {
			d.done = true
			return 0, io.EOF
		}
		return 0, fmt.Errorf("read nonce: %w", err)
	}
	if !nonceMatchesCounter(nonce, d.prefix[:], d.counter) {
		return 0, errors.New("backupcrypto: nonce/counter mismatch (re-ordered or tampered stream)")
	}
	// Try to read a full chunk first ; fall back to short.
	cipherBuf := make([]byte, d.chunkSize+tagLen)
	n, err := io.ReadFull(d.src, cipherBuf)
	switch {
	case err == nil:
		// Full chunk path.
	case err == io.ErrUnexpectedEOF:
		cipherBuf = cipherBuf[:n]
	default:
		return 0, fmt.Errorf("read ciphertext: %w", err)
	}
	plain, oerr := d.aead.Open(nil, nonce, cipherBuf, d.header)
	if oerr != nil {
		return 0, fmt.Errorf("backupcrypto: chunk %d failed to authenticate (key mismatch or tampered stream): %w", d.counter, oerr)
	}
	d.counter++
	d.pending = plain
	if len(cipherBuf) < d.chunkSize+tagLen {
		d.done = true
	}
	n2 := copy(p, d.pending)
	d.pending = d.pending[n2:]
	return n2, nil
}

func nonceMatchesCounter(nonce, prefix []byte, counter uint32) bool {
	for i := 0; i < streamPrefixLen; i++ {
		if nonce[i] != prefix[i] {
			return false
		}
	}
	return binary.BigEndian.Uint32(nonce[streamPrefixLen:streamPrefixLen+4]) == counter
}

func lookupUint(m map[string]string, key string, def uint64) uint64 {
	if v, ok := m[key]; ok {
		if u, err := strconv.ParseUint(v, 10, 64); err == nil && u > 0 {
			return u
		}
	}
	return def
}
