package backupcrypto

import (
	"bytes"
	"crypto/rand"
	"io"
	"strings"
	"testing"
)

// TestRoundTrip_FullStream proves Encoder + Decoder are perfectly
// inverse on a multi-chunk stream — the foundational guarantee that
// any backup taken can be restored exactly.
func TestRoundTrip_FullStream(t *testing.T) {
	for _, algo := range []string{AlgChaCha20Poly1305, AlgAES256GCM} {
		t.Run(algo, func(t *testing.T) {
			key := make([]byte, derivedKeyLen)
			_, _ = rand.Read(key)
			// 10 MiB payload covers ≥ 2 full 4 MiB chunks + a short tail.
			plain := make([]byte, 10*1024*1024)
			_, _ = rand.Read(plain)
			params := Params{Algorithm: algo, Key: key, ChunkSize: defaultChunkSize}
			var cipherBuf bytes.Buffer
			enc, err := NewEncoder(&cipherBuf, params)
			if err != nil {
				t.Fatalf("NewEncoder: %v", err)
			}
			if _, err := enc.Write(plain); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if err := enc.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if cipherBuf.Len() <= len(plain) {
				t.Errorf("ciphertext (%d B) should be larger than plaintext (%d B) due to nonces+tags",
					cipherBuf.Len(), len(plain))
			}
			dec, err := NewDecoder(&cipherBuf, params)
			if err != nil {
				t.Fatalf("NewDecoder: %v", err)
			}
			out, err := io.ReadAll(dec)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if !bytes.Equal(plain, out) {
				t.Errorf("round-trip mismatch ; out len=%d plain len=%d", len(out), len(plain))
			}
		})
	}
}

// TestRoundTrip_EmptyStream confirms a zero-byte backup round-trips
// without dropping the header (no nil-deref or magic mismatch).
func TestRoundTrip_EmptyStream(t *testing.T) {
	key := make([]byte, derivedKeyLen)
	params := Params{Algorithm: AlgChaCha20Poly1305, Key: key}
	var cipherBuf bytes.Buffer
	enc, _ := NewEncoder(&cipherBuf, params)
	if err := enc.Close(); err != nil {
		t.Fatalf("Close on empty: %v", err)
	}
	if cipherBuf.Len() != streamHeaderLen {
		t.Errorf("empty stream cipher len = %d, want %d (header only)", cipherBuf.Len(), streamHeaderLen)
	}
	dec, _ := NewDecoder(&cipherBuf, params)
	out, err := io.ReadAll(dec)
	if err != nil {
		t.Fatalf("ReadAll on empty: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("empty stream decoded to %d bytes", len(out))
	}
}

// TestDecode_WrongKey_FailsClearly proves a key mismatch surfaces as a
// clear auth error rather than as garbage output — critical for a
// backup system where silent corruption is the worst failure mode.
func TestDecode_WrongKey_FailsClearly(t *testing.T) {
	rightKey := make([]byte, derivedKeyLen)
	wrongKey := make([]byte, derivedKeyLen)
	_, _ = rand.Read(rightKey)
	_, _ = rand.Read(wrongKey)
	plain := []byte("operator-secret-content")
	var buf bytes.Buffer
	enc, _ := NewEncoder(&buf, Params{Algorithm: AlgChaCha20Poly1305, Key: rightKey})
	_, _ = enc.Write(plain)
	_ = enc.Close()
	dec, _ := NewDecoder(&buf, Params{Algorithm: AlgChaCha20Poly1305, Key: wrongKey})
	_, err := io.ReadAll(dec)
	if err == nil {
		t.Fatal("expected auth error on wrong key, got nil")
	}
	if !strings.Contains(err.Error(), "authenticate") {
		t.Errorf("error %q should mention 'authenticate' for operator clarity", err)
	}
}

// TestDecode_TamperedChunk_FailsClearly proves the integrity tag
// catches a flipped byte anywhere in the ciphertext.
func TestDecode_TamperedChunk_FailsClearly(t *testing.T) {
	key := make([]byte, derivedKeyLen)
	_, _ = rand.Read(key)
	plain := bytes.Repeat([]byte("A"), 8*1024*1024)
	params := Params{Algorithm: AlgChaCha20Poly1305, Key: key}
	var buf bytes.Buffer
	enc, _ := NewEncoder(&buf, params)
	_, _ = enc.Write(plain)
	_ = enc.Close()
	tampered := buf.Bytes()
	// Flip a byte deep in the first chunk's ciphertext (past header+nonce).
	tampered[streamHeaderLen+nonceLen+100] ^= 0x01
	dec, _ := NewDecoder(bytes.NewReader(tampered), params)
	_, err := io.ReadAll(dec)
	if err == nil {
		t.Fatal("tampered chunk should fail to authenticate")
	}
}

// TestDeriveKey_Argon2idDeterministic confirms the same passphrase + salt
// always produces the same key — the foundational property restore needs.
func TestDeriveKey_Argon2idDeterministic(t *testing.T) {
	passphrase := []byte("hunter2")
	salt := []byte("16-byte-salt-OK!")
	k1, err := DeriveKey(passphrase, salt, KDFArgon2id, nil)
	if err != nil {
		t.Fatal(err)
	}
	k2, err := DeriveKey(passphrase, salt, KDFArgon2id, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(k1, k2) {
		t.Error("argon2id produced different keys for the same (passphrase, salt)")
	}
	if len(k1) != derivedKeyLen {
		t.Errorf("derived key len = %d, want %d", len(k1), derivedKeyLen)
	}
}

// TestDeriveKey_RawRequiresHexCorrectLength documents the kdf=raw
// invariants for KMS-managed deployments.
func TestDeriveKey_RawRequiresHexCorrectLength(t *testing.T) {
	// 64 hex chars = 32 bytes = correct.
	good := bytes.Repeat([]byte("a"), 64)
	if _, err := DeriveKey(good, nil, KDFRaw, nil); err != nil {
		t.Errorf("good raw key rejected: %v", err)
	}
	// Wrong length.
	short := bytes.Repeat([]byte("a"), 32)
	if _, err := DeriveKey(short, nil, KDFRaw, nil); err == nil {
		t.Error("short raw key should be rejected")
	}
	// Not hex.
	notHex := []byte(strings.Repeat("z", 64))
	if _, err := DeriveKey(notHex, nil, KDFRaw, nil); err == nil {
		t.Error("non-hex raw key should be rejected")
	}
}

// TestNewSalt_NonZero_Length ensures the salt is what the format
// expects and isn't trivially predictable.
func TestNewSalt_NonZero_Length(t *testing.T) {
	s, err := NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	if len(s) != saltLen {
		t.Errorf("salt len = %d, want %d", len(s), saltLen)
	}
	var zero [saltLen]byte
	if bytes.Equal(s, zero[:]) {
		t.Error("salt is all zeros — random source broken?")
	}
}
