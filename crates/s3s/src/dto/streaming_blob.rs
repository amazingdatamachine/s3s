//! Streaming blob

use crate::error::StdError;
use crate::http::Body;
use crate::stream::*;

use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{AsyncRead, ready, Stream, TryStream, TryStreamExt};
use hyper::body::Bytes;
// use pin_project_lite::pin_project;
use std::cmp;

pub struct StreamingBlob {
    inner: DynByteStream,
}

impl StreamingBlob {
    pub fn new<S>(stream: S) -> Self
    where
        S: ByteStream<Item = Result<Bytes, StdError>> + Send + Sync + 'static,
    {
        Self { inner: Box::pin(stream) }
    }

    pub fn wrap<S, E>(stream: S) -> Self
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Sync + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        Self { inner: wrap(stream) }
    }

    fn into_inner(self) -> DynByteStream {
        self.inner
    }
}

impl fmt::Debug for StreamingBlob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingBlob")
            .field("remaining_length", &self.remaining_length())
            .finish_non_exhaustive()
    }
}

impl Stream for StreamingBlob {
    type Item = Result<Bytes, StdError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ByteStream for StreamingBlob {
    fn remaining_length(&self) -> RemainingLength {
        self.inner.remaining_length()
    }
}

impl From<StreamingBlob> for DynByteStream {
    fn from(value: StreamingBlob) -> Self {
        value.into_inner()
    }
}

impl From<DynByteStream> for StreamingBlob {
    fn from(value: DynByteStream) -> Self {
        Self { inner: value }
    }
}

impl From<StreamingBlob> for Body {
    fn from(value: StreamingBlob) -> Self {
        Body::from(value.into_inner())
    }
}

impl From<Body> for StreamingBlob {
    fn from(value: Body) -> Self {
        Self::new(value)
    }
}

pin_project_lite::pin_project! {
    pub struct StreamWrapper<S> {
        #[pin]
        inner: S
    }
}

impl<S, E> Stream for StreamWrapper<S>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Sync + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    type Item = Result<Bytes, StdError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.poll_next(cx).map_err(|e| Box::new(e) as StdError)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<S> ByteStream for StreamWrapper<S>
where
    StreamWrapper<S>: Stream<Item = Result<Bytes, StdError>>,
{
    fn remaining_length(&self) -> RemainingLength {
        RemainingLength::unknown()
    }
}

fn wrap<S>(inner: S) -> DynByteStream
where
    StreamWrapper<S>: ByteStream<Item = Result<Bytes, StdError>> + Send + Sync + 'static,
{
    Box::pin(StreamWrapper { inner })
}

pin_project_lite::pin_project! {
    #[derive(Debug)]
    pub struct IntoAsyncRead2<St>
    where
        St: TryStream<Error = StdError>,
        St::Ok: AsRef<[u8]>,
    {
        #[pin]
        stream: St,
        state: ReadState<St::Ok>,
    }
}

#[derive(Debug)]
enum ReadState<T: AsRef<[u8]>> {
    Ready { chunk: T, chunk_start: usize },
    PendingChunk,
    Eof,
}

impl<St> IntoAsyncRead2<St>
    where
        St: TryStream<Error = StdError>,
        St::Ok: AsRef<[u8]>,
{
    pub fn new(stream: St) -> Self {
        Self { stream, state: ReadState::PendingChunk }
    }
}

impl<St> AsyncRead for IntoAsyncRead2<St>
    where
        St: TryStream<Error = StdError>,
        St::Ok: AsRef<[u8]>,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut this = self.project();

        loop {
            match this.state {
                ReadState::Ready { chunk, chunk_start } => {
                    let chunk = chunk.as_ref();
                    let len = cmp::min(buf.len(), chunk.len() - *chunk_start);

                    buf[..len].copy_from_slice(&chunk[*chunk_start..*chunk_start + len]);
                    *chunk_start += len;

                    if chunk.len() == *chunk_start {
                        *this.state = ReadState::PendingChunk;
                    }

                    return Poll::Ready(Ok(len));
                }
                ReadState::PendingChunk => match ready!(this.stream.as_mut().try_poll_next(cx)) {
                    Some(Ok(chunk)) => {
                        if !chunk.as_ref().is_empty() {
                            *this.state = ReadState::Ready { chunk, chunk_start: 0 };
                        }
                    }
                    Some(Err(err)) => {
                        *this.state = ReadState::Eof;
                        return Poll::Ready(Err(std::io::Error::other(err.to_string())));
                    }
                    None => {
                        *this.state = ReadState::Eof;
                        return Poll::Ready(Ok(0));
                    }
                },
                ReadState::Eof => {
                    return Poll::Ready(Ok(0));
                }
            }
        }
    }
}
