# smoosh

automagic recompression for Rust! smoosh autodetects input compression and
recompresses it into whatever format you want!

## usage

```rust
smoosh::recompress(&mut input_stream, &mut output_stream, smoosh::CompressionType::Zstd).await?;
// that's it!
```
