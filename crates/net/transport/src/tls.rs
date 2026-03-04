//! TLS configuration helpers for QUIC transport endpoints.

use std::sync::Arc;

use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

use crate::error::TransportError;

/// Generate a self-signed TLS certificate for QUIC connections.
///
/// The publisher ↔ sidecar link uses self-signed certs because both endpoints
/// are operated by the same party.
pub fn generate_self_signed(
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), TransportError> {
    let cert = generate_simple_self_signed(vec!["localhost".to_string()])
        .map_err(|e| TransportError::Tls(e.to_string()))?;

    let cert_der = CertificateDer::from(cert.cert);
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()));

    Ok((vec![cert_der], key_der))
}

/// ALPN protocol identifier for publisher ↔ sidecar QUIC connections.
const ALPN_COMPOSE_QUIC: &[u8] = b"compose-quic";

/// Build a `rustls::ClientConfig` that accepts any server certificate.
///
/// This is intentional: the sidecar connects to a known publisher whose
/// identity is established out of band (config), not via the PKI.
pub fn insecure_client_config() -> Result<Arc<rustls::ClientConfig>, TransportError> {
    let mut config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerifier))
        .with_no_client_auth();
    config.alpn_protocols = vec![ALPN_COMPOSE_QUIC.to_vec()];
    Ok(Arc::new(config))
}

/// Build a `rustls::ServerConfig` using a self-signed certificate.
pub fn self_signed_server_config() -> Result<Arc<rustls::ServerConfig>, TransportError> {
    let (certs, key) = generate_self_signed()?;
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| TransportError::Tls(e.to_string()))?;
    Ok(Arc::new(config))
}

/// Certificate verifier that accepts any certificate. Used for self-signed
/// connections where the peer identity is established via the application
/// layer.
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
        ]
    }
}
