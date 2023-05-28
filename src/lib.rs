use async_compression::tokio::bufread::{
    BzDecoder, DeflateDecoder, GzipDecoder, XzDecoder, ZlibDecoder, ZstdDecoder,
};
use async_compression::tokio::write::{
    BzEncoder, DeflateEncoder, GzipEncoder, XzEncoder, ZlibEncoder, ZstdEncoder,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

pub async fn recompress<
    'a,
    R: AsyncRead + std::marker::Unpin + Send,
    W: AsyncWrite + std::marker::Unpin + Send,
>(
    input: &mut R,
    output: &mut W,
    output_type: CompressionType,
) -> std::io::Result<()> {
    let (input_type, magic) = detect_stream_characteristics(input).await?;
    let input = &mut magic.chain(input);

    if input_type == output_type {
        tokio::io::copy(input, output).await?;
        return Ok(());
    }

    let mut decompressor: Box<dyn AsyncRead + std::marker::Unpin + Send> = match input_type {
        CompressionType::Bzip => Box::new(BzDecoder::new(BufReader::new(input))),
        CompressionType::Deflate => Box::new(DeflateDecoder::new(BufReader::new(input))),
        CompressionType::Gzip => Box::new(GzipDecoder::new(BufReader::new(input))),
        CompressionType::Xz => Box::new(XzDecoder::new(BufReader::new(input))),
        CompressionType::Zlib => Box::new(ZlibDecoder::new(BufReader::new(input))),
        CompressionType::Zstd => Box::new(ZstdDecoder::new(BufReader::new(input))),
        CompressionType::None => Box::new(BufReader::new(input)),
    };

    let mut recompressor: Box<dyn AsyncWrite + std::marker::Unpin + Send> = match output_type {
        CompressionType::Bzip => Box::new(BzEncoder::new(output)),
        CompressionType::Deflate => Box::new(DeflateEncoder::new(output)),
        CompressionType::Gzip => Box::new(GzipEncoder::new(output)),
        CompressionType::Xz => Box::new(XzEncoder::new(output)),
        CompressionType::Zlib => Box::new(ZlibEncoder::new(output)),
        CompressionType::Zstd => Box::new(ZstdEncoder::new(output)),
        CompressionType::None => Box::new(output),
    };

    tokio::io::copy(&mut decompressor, &mut recompressor).await?;
    recompressor.flush().await?;

    Ok(())
}

async fn detect_stream_characteristics<R: AsyncRead + std::marker::Unpin>(
    stream: &mut R,
) -> std::io::Result<(CompressionType, Vec<u8>)> {
    let mut buffer = [0; 6];
    let _n = stream.read(&mut buffer).await?;
    let kind = detect_compression_type(&buffer);

    Ok((kind, Vec::from(buffer)))
}

fn detect_compression_type(buffer: &[u8; 6]) -> CompressionType {
    match buffer {
        [0x28, 0xb5, 0x2f, 0xfd, _, _] => CompressionType::Zstd,
        [0x1f, 0x8b, _, _, _, _] => CompressionType::Gzip,
        [0x78, 0x01, _, _, _, _] => CompressionType::Deflate,
        [0x78, 0x9c, _, _, _, _] => CompressionType::Zlib,
        [0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00] => CompressionType::Xz,
        [0x42, 0x5a, 0x68, _, _, _] => CompressionType::Bzip,
        _ => CompressionType::None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Copy)]
pub enum CompressionType {
    Bzip,
    Deflate, //
    Gzip,    //
    Xz,      //
    Zlib,    //
    Zstd,    //
    None,    //
}

#[cfg(test)]
mod test {
    use std::io::Result;

    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn test_none_compression_works() -> Result<()> {
        let expected = "this is a test";
        let mut input_stream = expected.as_bytes();
        let mut output_stream: Vec<u8> = Vec::new();

        recompress(&mut input_stream, &mut output_stream, CompressionType::None).await?;

        assert_eq!(expected.as_bytes(), output_stream);

        Ok(())
    }

    #[tokio::test]
    async fn test_zstd_compression_works() -> Result<()> {
        let expected = "this is a test";
        let mut input_stream = expected.as_bytes();
        let mut output_stream: Vec<u8> = Vec::new();

        recompress(&mut input_stream, &mut output_stream, CompressionType::Zstd).await?;

        let mut compressed_stream: Vec<u8> = Vec::new();
        {
            let mut encoder = ZstdEncoder::new(&mut compressed_stream);
            encoder.write_all(expected.as_bytes()).await?;
            encoder.flush().await?;
        }

        assert!(!compressed_stream.is_empty());
        assert_eq!(compressed_stream, output_stream);
        assert_ne!(expected.as_bytes(), output_stream);

        Ok(())
    }

    #[tokio::test]
    async fn test_gzip_compression_works() -> Result<()> {
        let expected = "this is a test";
        let mut input_stream = expected.as_bytes();
        let mut output_stream: Vec<u8> = Vec::new();

        recompress(&mut input_stream, &mut output_stream, CompressionType::Gzip).await?;

        let mut compressed_stream: Vec<u8> = Vec::new();
        {
            let mut encoder = GzipEncoder::new(&mut compressed_stream);
            encoder.write_all(expected.as_bytes()).await?;
            encoder.flush().await?;
        }

        assert!(!compressed_stream.is_empty());
        assert_eq!(compressed_stream, output_stream);
        assert_ne!(expected.as_bytes(), output_stream);

        Ok(())
    }

    #[tokio::test]
    async fn test_deflate_compression_works() -> Result<()> {
        let expected = "this is a test";
        let mut input_stream = expected.as_bytes();
        let mut output_stream: Vec<u8> = Vec::new();

        recompress(
            &mut input_stream,
            &mut output_stream,
            CompressionType::Deflate,
        )
        .await?;

        let mut compressed_stream: Vec<u8> = Vec::new();
        {
            let mut encoder = DeflateEncoder::new(&mut compressed_stream);
            encoder.write_all(expected.as_bytes()).await?;
            encoder.flush().await?;
        }

        assert!(!compressed_stream.is_empty());
        assert_eq!(compressed_stream, output_stream);
        assert_ne!(expected.as_bytes(), output_stream);

        Ok(())
    }

    #[tokio::test]
    async fn test_zlib_compression_works() -> Result<()> {
        let expected = "this is a test";
        let mut input_stream = expected.as_bytes();
        let mut output_stream: Vec<u8> = Vec::new();

        recompress(&mut input_stream, &mut output_stream, CompressionType::Zlib).await?;

        let mut compressed_stream: Vec<u8> = Vec::new();
        {
            let mut encoder = ZlibEncoder::new(&mut compressed_stream);
            encoder.write_all(expected.as_bytes()).await?;
            encoder.flush().await?;
        }

        assert!(!compressed_stream.is_empty());
        assert_eq!(compressed_stream, output_stream);
        assert_ne!(expected.as_bytes(), output_stream);

        Ok(())
    }

    #[tokio::test]
    async fn test_xz_compression_works() -> Result<()> {
        let expected = "this is a test";
        let mut input_stream = expected.as_bytes();
        let mut output_stream: Vec<u8> = Vec::new();

        recompress(&mut input_stream, &mut output_stream, CompressionType::Xz).await?;

        let mut compressed_stream: Vec<u8> = Vec::new();
        {
            let mut encoder = XzEncoder::new(&mut compressed_stream);
            encoder.write_all(expected.as_bytes()).await?;
            encoder.flush().await?;
        }

        assert!(!compressed_stream.is_empty());
        assert_eq!(compressed_stream, output_stream);
        assert_ne!(expected.as_bytes(), output_stream);

        Ok(())
    }
}
