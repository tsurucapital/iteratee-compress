{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ForeignFunctionInterface #-}
module Data.Iteratee.ZLib
  (
    -- * Enumerators
    enumInflate,
    enumDeflate,
    -- * Exceptions
    ZLibParamsException(..),
    ZLibException(..),
    -- * Parameters
    CompressParams(..),
    defaultCompressParams,
    DecompressParams(..),
    defaultDecompressParams,
    Format(..),
    CompressionLevel(..),
    Method(..),
    WindowBits(..),
    MemoryLevel(..),
    CompressionStrategy(..),
  )
where
#include <zlib.h>

import Control.Applicative
import Control.Exception
import Control.Monad.Trans
import Data.ByteString as BS
import Data.ByteString.Internal
import Data.Iteratee
import Data.Iteratee.IO
import Data.Typeable
import Foreign
import Foreign.C
#ifdef DEBUG
import qualified Foreign.Concurrent as C
import System.IO (stderr)
import qualified System.IO as IO
#endif

-- | Denotes error is user-supplied parameter
data ZLibParamsException
    = IncorrectCompressionLevel !Int
    -- ^ Incorrect compression level was chosen
    | IncorrectWindowBits !Int
    -- ^ Incorrect number of window bits was chosen
    | IncorrectMemoryLevel !Int
    -- ^ Incorrect memory level was chosen
    deriving (Eq,Typeable)

-- | Denotes error in compression and decompression
data ZLibException
    = NeedDictionary
    -- ^ Decompression requires user-supplied dictionary (not supported)
    | BufferError
    -- ^ Buffer error - denotes a library error
--    | File Error
    | StreamError
    -- ^ State of steam inconsistent
    | DataError
    -- ^ Input data corrupted
    | MemoryError
    -- ^ Not enough memory
    | VersionError
    -- ^ Version error
    | Unexpected !CInt
    -- ^ Unexpected or unknown error - please report as bug
    | IncorrectState
    -- ^ Incorrect state - denotes error in library
    deriving (Eq,Typeable)

-- | Denotes the flush that can be sent to stream
data ZlibFlush
    = SyncFlush
    -- ^ All pending output is flushed and all input that is available is sent
    -- to inner Iteratee.
    | FullFlush
    -- ^ Flush all pending output and reset the compression state. It allows to
    -- restart from this point if compression was damaged but it can seriously 
    -- affect the compression rate.
    --
    -- It may be only used during compression.
    | Block
    -- ^ If the iteratee is compressing it requests to stop when next block is
    -- emmited. On the beginning it skips only header if and only if it exists.
    deriving (Eq,Typeable)

instance Show ZlibFlush where
    show SyncFlush = "zlib: flush requested"
    show FullFlush = "zlib: full flush requested"
    show Block = "zlib: block flush requested"

instance Exception ZlibFlush

fromFlush :: ZlibFlush -> CInt
fromFlush SyncFlush = #{const Z_SYNC_FLUSH}
fromFlush FullFlush = #{const Z_FULL_FLUSH}
fromFlush Block = #{const Z_BLOCK}

instance Show ZLibParamsException where
    show (IncorrectCompressionLevel lvl)
        = "zlib: incorrect compression level " ++ show lvl
    show (IncorrectWindowBits lvl)
        = "zlib: incorrect window bits " ++ show lvl
    show (IncorrectMemoryLevel lvl)
        = "zlib: incorrect memory level " ++ show lvl

instance Show ZLibException where
    show NeedDictionary = "zlib: needs dictionary"
    show BufferError = "zlib: no progress is possible (internal error)"
--    show FileError = "zlib: file I/O error"
    show StreamError = "zlib: stream error"
    show DataError = "zlib: data error"
    show MemoryError = "zlib: memory error"
    show VersionError = "zlib: version error"
    show (Unexpected lvl) = "zlib: unknown error " ++ show lvl
    show IncorrectState = "zlib: incorrect state"

instance Exception ZLibParamsException
instance Exception ZLibException 

newtype ZStream = ZStream (ForeignPtr ZStream)
withZStream :: ZStream -> (Ptr ZStream -> IO a) -> IO a
withZStream (ZStream fptr) = withForeignPtr fptr

mallocZStream :: IO ZStream
mallocZStream = ZStream <$> mallocForeignPtrBytes #{size z_stream}

-- Following code is copied from Duncan Coutts zlib haskell library version
-- 0.5.2.0 ((c) 2006-2008 Duncan Coutts, published on BSD licence) and adapted

-- | Set of parameters for compression. For sane defaults use
-- 'defaultCompressParams'
data CompressParams = CompressParams {
      compressLevel :: !CompressionLevel,
      compressMethod :: !Method,
      compressWindowBits :: !WindowBits,
      compressMemoryLevel :: !MemoryLevel,
      compressStrategy :: !CompressionStrategy,
      -- | The size of output buffer. That is the size of 'Chunk's that will be
      -- emitted to inner iterator (except the last 'Chunk').
      compressBufferSize :: !Int
    }

defaultCompressParams
    = CompressParams DefaultCompression Deflated DefaultWindowBits 
                     DefaultMemoryLevel DefaultStrategy (8*1024)

-- | Set of parameters for decompression. For sane defaults see 
-- 'defaultDecompressParams'.
data DecompressParams = DecompressParams {
      -- | Window size - it have to be at least the size of
      -- 'compressWindowBits' the stream was compressed with.
      --
      -- Default in 'defaultDecompressParams' is the maximum window size - 
      -- please do not touch it unless you know what you are doing.
      decompressWindowBits :: !WindowBits,
      -- | The size of output buffer. That is the size of 'Chunk's that will be
      -- emitted to inner iterator (except the last 'Chunk').
      decompressBufferSize :: !Int
    }

defaultDecompressParams = DecompressParams DefaultWindowBits (8*1024)

-- | Specify the format for compression and decompression
data Format
    = GZip
    -- ^ The gzip format is widely used and uses a header with checksum and
    -- some optional metadata about the compress file.
    --
    -- It is intended primarily for compressing individual files but is also
    -- used for network protocols such as HTTP.
    --
    -- The format is described in RFC 1952 
    -- <http://www.ietf.org/rfc/rfc1952.txt>.
    | Zlib
    -- ^ The zlib format uses a minimal header with a checksum but no other
    -- metadata. It is designed for use in network protocols.
    --
    -- The format is described in RFC 1950
    -- <http://www.ietf.org/rfc/rfc1950.txt>
    | Raw
    -- ^ The \'raw\' format is just the DEFLATE compressed data stream without
    -- and additionl headers. 
    --
    -- Thr format is described in RFC 1951
    -- <http://www.ietf.org/rfc/rfc1951.txt>
    | GZipOrZlib
    -- ^ "Format" for decompressing a 'Zlib' or 'GZip' stream.
    deriving (Eq)

-- | The compression level specify the tradeoff between speed and compression.
data CompressionLevel
    = DefaultCompression
    -- ^ Default compression level set at 6.
    | NoCompression
    -- ^ No compression, just a block copy.
    | BestSpeed
    -- ^ The fastest compression method (however less compression)
    | BestCompression
    -- ^ The best compression method (however slowest)
    | CompressionLevel Int
    -- ^ Compression level set by number from 1 to 9

-- | Specify the compression method.
data Method
    = Deflated
    -- ^ \'Deflate\' is so far the only method supported.          

-- | This specify the size of compression level. Larger values result in better
-- compression at the expense of highier memory usage.
--
-- The compression window size is 2 to the power of the value of the window
-- bits. 
--
-- The total memory used depends on windows bits and 'MemoryLevel'.
data WindowBits
    = WindowBits Int
    -- ^ The size of window bits. It have to be between @8@ (which corresponds
    -- to 256b i.e. 32B) and @15@ (which corresponds to 32 kib i.e. 4kiB).
    | DefaultWindowBits
    -- ^ The default window size which is 4kiB

-- | The 'MemoryLevel' specifies how much memory should be allocated for the
-- internal state. It is a tradeoff between memory usage, speed and
-- compression.
-- Using more memory allows faster and better compression.
--
-- The memory used for interal state, excluding 'WindowBits', is 512 bits times
-- 2 to power of memory level.
--
-- The total amount of memory use depends on the 'WindowBits' and
-- 'MemoryLevel'.
data MemoryLevel
    = DefaultMemoryLevel
    -- ^ Default memory level set to 8.
    | MinMemoryLevel
    -- ^ Use the small amount of memory (equivalent to memory level 1) - i.e.
    -- 1024b or 256 B.
    -- It slow and reduces the compresion ratio.
    | MaxMemoryLevel
    -- ^ Maximum memory level for optimal compression speed (equivalent to
    -- memory level 9).
    -- The internal state is 256kib or 32kiB.
    | MemoryLevel Int
    -- ^ A specific level. It have to be between 1 and 9. 

-- | Tunes the compress algorithm but does not affact the correctness.
data CompressionStrategy
    = DefaultStrategy
    -- ^ Default strategy
    | Filtered
    -- ^ Use the filtered compression strategy for data produced by a filter
    -- (or predictor). Filtered data consists mostly of small values with a
    -- somewhat random distribution. In this case, the compression algorithm
    -- is tuned to compress them better. The effect of this strategy is to
    -- force more Huffman coding and less string matching; it is somewhat
    -- intermediate between 'DefaultStrategy' and 'HuffmanOnly'.
    | HuffmanOnly
    -- ^ Use the Huffman-only compression strategy to force Huffman encoding
    -- only (no string match). 

fromMethod :: Method -> CInt
fromMethod Deflated = #{const Z_DEFLATED}

fromCompressionLevel :: CompressionLevel -> Either ZLibParamsException CInt
fromCompressionLevel DefaultCompression = Right $! -1
fromCompressionLevel NoCompression = Right $! 0
fromCompressionLevel BestSpeed = Right $! 1
fromCompressionLevel BestCompression = Right $! 9
fromCompressionLevel (CompressionLevel n)
    | n >= 0 && n <= 9 = Right $! fromIntegral $! n
    | otherwise = Left $! IncorrectCompressionLevel n

fromWindowBits :: Format -> WindowBits -> Either ZLibParamsException CInt
fromWindowBits format bits
    = formatModifier format <$> checkWindowBits bits
    where checkWindowBits DefaultWindowBits = Right $! 15
          checkWindowBits (WindowBits n)
              | n >= 8 && n <= 15 = Right $! fromIntegral $! n
              | otherwise = Left $! IncorrectWindowBits $! n
          formatModifier Zlib       = id
          formatModifier GZip       = (+16)
          formatModifier GZipOrZlib = (+32)
          formatModifier Raw        = negate

fromMemoryLevel :: MemoryLevel -> Either ZLibParamsException CInt
fromMemoryLevel DefaultMemoryLevel = Right $! 8
fromMemoryLevel MinMemoryLevel     = Right $! 1
fromMemoryLevel MaxMemoryLevel     = Right $! 9
fromMemoryLevel (MemoryLevel n)
         | n >= 1 && n <= 9 = Right $! fromIntegral n
         | otherwise        = Left $! IncorrectMemoryLevel $! fromIntegral n

fromCompressionStrategy :: CompressionStrategy -> CInt
fromCompressionStrategy DefaultStrategy = #{const Z_DEFAULT_STRATEGY}
fromCompressionStrategy Filtered        = #{const Z_FILTERED}
fromCompressionStrategy HuffmanOnly     = #{const Z_HUFFMAN_ONLY}

fromErrno :: CInt -> Either ZLibException Bool
fromErrno (#{const Z_OK}) = Right $! True
fromErrno (#{const Z_STREAM_END}) = Right $! False
fromErrno (#{const Z_NEED_DICT}) = Left $! NeedDictionary
fromErrno (#{const Z_BUF_ERROR}) = Left $! BufferError
--fromErrno (#{const Z_ERRNO}) = Left $! FileError
fromErrno (#{const Z_STREAM_ERROR}) = Left $! StreamError
fromErrno (#{const Z_DATA_ERROR}) = Left $! DataError
fromErrno (#{const Z_MEM_ERROR}) = Left $! MemoryError
fromErrno (#{const Z_VERSION_ERROR}) = Left $! VersionError
fromErrno n = Left $! Unexpected n

-- Helper function
convParam :: Format
          -> CompressParams
          -> Either ZLibParamsException (CInt, CInt, CInt, CInt, CInt)
convParam f (CompressParams c m w l s _)
    = let c' = fromCompressionLevel c
          m' = fromMethod m
          b' = fromWindowBits f w
          l' = fromMemoryLevel l
          s' = fromCompressionStrategy s
          eit = either Left
          r = Right
      in eit (\c_ -> eit (\b_ -> eit (\l_ -> r (c_, m', b_, l_, s')) l') b') c'
--
-- In following code we go through 7 states. Some of the operations are
-- 'deterministic' like 'insertOut' and some of them depends on input ('fill')
-- or library call.
--
--                                                  (Finished)
--                                                     ^
--                                                     |
--                                                     |
--                                                     | finish
--                                                     |
--              insertOut                fill[1]       |
---  (Initial) -------------> (EmptyIn) -----------> (Finishing)
--         ^                    ^ | ^ |
--         |             run[2] | | | \------------------\
--         |                    | | |                    |
--         |                    | | \------------------\ |
--         |    run[1]          | |        flush[0]    | |
--         \------------------\ | | fill[0]            | | fill[3]
--                            | | |                    | |
--                            | | |                    | |
--               swapOut      | | v       flush[1]     | v
--  (FullOut) -------------> (Invalid) <----------- (Flushing)
--
-- Initial: Initial state, both buffers are empty
-- EmptyIn: Empty in buffer, out waits untill filled
-- FullOut: Out was filled and sent. In was not entirely read
-- Invalid[1]: Both buffers non-empty
-- Finishing: There is no more in data and in buffer is empty. Waits till
--    all outs was sent.
-- Finished: Operation finished
-- Flushing: Flush requested
-- 
-- Please note that the decompressing can finish also on flush and finish.
--
-- [1] Named for 'historical' reasons

newtype Initial = Initial ZStream
data EmptyIn = EmptyIn !ZStream !ByteString
data FullOut = FullOut !ZStream !ByteString
data Invalid = Invalid !ZStream !ByteString !ByteString
data Finishing = Finishing !ZStream !ByteString
data Flushing = Flushing !ZStream !ZlibFlush !ByteString

withByteString :: ByteString -> (Ptr Word8 -> Int -> IO a) -> IO a
withByteString (PS ptr off len) f
    = withForeignPtr ptr (\ptr' -> f (ptr' `plusPtr` off) len)

#ifdef DEBUG
mkByteString :: MonadIO m => Int -> m ByteString
mkByteString s = liftIO $ do
    base <- mallocForeignPtrArray s
    withForeignPtr base $ \ptr ->  C.addForeignPtrFinalizer base $ do
        IO.hPutStrLn stderr $ "Freed buffer " ++ show ptr
    IO.hPutStrLn stderr $ "Allocated buffer " ++ show base
    return $! PS base 0 s

dumpZStream :: ZStream -> IO ()
dumpZStream zstr = withZStream zstr $ \zptr -> do
    IO.hPutStr stderr $ "<<ZStream@"
    IO.hPutStr stderr $ (show zptr)
    IO.hPutStr stderr . (" next_in=" ++) . show =<<
        (#{peek z_stream, next_in} zptr :: IO (Ptr ()))
    IO.hPutStr stderr . (" avail_in=" ++) . show =<<
        (#{peek z_stream, avail_in} zptr :: IO CUInt)
    IO.hPutStr stderr . (" total_in=" ++) . show =<<
        (#{peek z_stream, total_in} zptr :: IO CULong)
    IO.hPutStr stderr . (" next_out=" ++) . show =<<
        (#{peek z_stream, next_out} zptr :: IO (Ptr ()))
    IO.hPutStr stderr . (" avail_out=" ++) . show =<<
        (#{peek z_stream, avail_out} zptr :: IO CUInt)
    IO.hPutStr stderr .  (" total_out=" ++) . show =<<
        (#{peek z_stream, total_out} zptr :: IO CULong)
--    IO.hPutStr stderr . (" msg=" ++) =<< peekCString =<<
--        (#{peek z_stream, msg} zptr)
    IO.hPutStrLn stderr ">>"
#else
mkByteString :: MonadIO m => Int -> m ByteString
mkByteString s = liftIO $ create s (\_ -> return ())
#endif

putOutBuffer :: Int -> ZStream -> IO ByteString
putOutBuffer size zstr = do
    _out <- mkByteString size
    withByteString _out $ \ptr len -> withZStream zstr $ \zptr -> do
        #{poke z_stream, next_out} zptr ptr
        #{poke z_stream, avail_out} zptr len
    return _out

putInBuffer :: ZStream -> ByteString -> IO ()
putInBuffer zstr _in
    = withByteString _in $ \ptr len -> withZStream zstr $ \zptr -> do
        #{poke z_stream, next_in} zptr ptr
        #{poke z_stream, avail_in} zptr len

pullOutBuffer :: ZStream -> ByteString -> IO ByteString
pullOutBuffer zstr _out = withByteString _out $ \ptr _ -> do
    next_out <- withZStream zstr $ \zptr -> #{peek z_stream, next_out} zptr
    return $! BS.take (next_out `minusPtr` ptr) _out

pullInBuffer :: ZStream -> ByteString -> IO ByteString
pullInBuffer zstr _in = withByteString _in $ \ptr _ -> do
    next_in <- withZStream zstr $ \zptr -> #{peek z_stream, next_in} zptr
    return $! BS.drop (next_in `minusPtr` ptr) _in

insertOut :: MonadIO m
          => Int
          -> (ZStream -> CInt -> IO CInt)
          -> Initial
          -> Enumeratee ByteString ByteString m a
insertOut size run (Initial zstr) iter = do
    _out <- liftIO $ putOutBuffer size zstr
#ifdef DEBUG
    liftIO $ IO.hPutStrLn stderr $ "Inserted out buffer of size " ++ show size
#endif
    fill size run (EmptyIn zstr _out) iter

fill :: MonadIO m
     => Int
     -> (ZStream -> CInt -> IO CInt)
     -> EmptyIn
     -> Enumeratee ByteString ByteString m a
fill size run (EmptyIn zstr _out) iter
    = let fill' (Chunk _in)
              | not (BS.null _in) = do
                  liftIO $ putInBuffer zstr _in
#ifdef DEBUG
                  liftIO $ IO.hPutStrLn stderr $
                      "Inserted in buffer of size " ++ show (BS.length _in)
#endif
                  doRun size run (Invalid zstr _in _out) iter
              | otherwise = fillI
          fill' (EOF Nothing) = do
              out <- liftIO $ pullOutBuffer zstr _out
              iter' <- lift $ enumPure1Chunk out iter
              finish size run (Finishing zstr BS.empty) iter'
          fill' (EOF (Just err))
              = case fromException err of
                  Just err' -> flush size run (Flushing zstr err' _out) iter
                  Nothing -> throwRecoverableErr err fill'
#ifdef DEBUG
          fillI = do
              liftIO $ IO.hPutStrLn stderr $ "About to insert in buffer"
              liftI fill'
#else
          fillI = liftI fill'
#endif
      in fillI

swapOut :: MonadIO m
        => Int
        -> (ZStream -> CInt -> IO CInt)
        -> FullOut
        -> Enumeratee ByteString ByteString m a
swapOut size run (FullOut zstr _in) iter = do
    _out <- liftIO $ putOutBuffer size zstr
#ifdef DEBUG
    liftIO $ IO.hPutStrLn stderr $ "Swapped out buffer of size " ++ show size
#endif
    doRun size run (Invalid zstr _in _out) iter

doRun :: MonadIO m
      => Int
      -> (ZStream -> CInt -> IO CInt)
      -> Invalid
      -> Enumeratee ByteString ByteString m a
doRun size run (Invalid zstr _in _out) iter = do
#ifdef DEBUG
    liftIO $ IO.hPutStrLn stderr $ "About to run"
    liftIO $ dumpZStream zstr
#endif
    status <- liftIO $ run zstr #{const Z_NO_FLUSH}
#ifdef DEBUG
    liftIO $ IO.hPutStrLn stderr $ "Runned"
#endif
    case fromErrno status of
        Left err -> do
            _ <- joinIM $ enumErr err iter
            throwErr (toException err)
        Right False -> do -- End of stream
            remaining <- liftIO $ pullInBuffer zstr _in
            out <- liftIO $ pullOutBuffer zstr _out
            iter' <- lift $ enumPure1Chunk out iter
            idone iter' (Chunk remaining)
        Right True -> do -- Continue
            (avail_in, avail_out) <- liftIO $ withZStream zstr $ \zptr -> do
                avail_in <- liftIO $ #{peek z_stream, avail_in} zptr
                avail_out <- liftIO $ #{peek z_stream, avail_out} zptr
                return (avail_in, avail_out) :: IO (CInt, CInt)
            case avail_out of
                0 -> do
                    out <- liftIO $ pullOutBuffer zstr _out
                    iter' <- lift $ enumPure1Chunk out iter
                    case avail_in of
                        0 -> insertOut size run (Initial zstr) iter'
                        _ -> swapOut size run (FullOut zstr _in) iter'
                _ -> case avail_in of
                    0 -> fill size run (EmptyIn zstr _out) iter
                    _ -> do
                        _ <- joinIM $ enumErr IncorrectState iter
                        throwErr (toException IncorrectState)

flush :: MonadIO m
      => Int
      -> (ZStream -> CInt -> IO CInt)
      -> Flushing
      -> Enumeratee ByteString ByteString m a
flush size run fin@(Flushing zstr _flush _out) iter = do
    status <- liftIO $ run zstr (fromFlush _flush)
    case fromErrno status of
        Left err -> do
            _ <- joinIM $ enumErr err iter
            throwErr (toException err)
        Right False -> do -- Finished
            out <- liftIO $ pullOutBuffer zstr _out
            iter' <- lift $ enumPure1Chunk out iter
            idone iter' (Chunk BS.empty)
        Right True -> do
            (avail_in, avail_out) <- liftIO $ withZStream zstr $ \zptr -> do
                avail_in <- liftIO $ #{peek z_stream, avail_in} zptr
                avail_out <- liftIO $ #{peek z_stream, avail_out} zptr
                return (avail_in, avail_out) :: IO (CInt, CInt)
            case avail_out of
                0 -> do
                    out <- liftIO $ pullOutBuffer zstr _out
                    iter' <- lift $ enumPure1Chunk out iter
                    out' <- liftIO $ putOutBuffer size zstr
                    flush size run (Flushing zstr _flush out') iter'
                _ -> insertOut size run (Initial zstr) iter

finish :: MonadIO m
       => Int
       -> (ZStream -> CInt -> IO CInt)
       -> Finishing
       -> Enumeratee ByteString ByteString m a
finish size run fin@(Finishing zstr _in) iter = do
#ifdef DEBUG
    liftIO $ IO.hPutStrLn stderr $
        "Finishing with out buffer of size " ++ show size
#endif
    _out <- liftIO $ putOutBuffer size zstr
    status <- liftIO $ run zstr #{const Z_FINISH}
    case fromErrno status of
        Left err -> do
            _ <- lift $ enumErr err iter
            throwErr (toException err)
        Right False -> do -- Finished
            remaining <- liftIO $ pullInBuffer zstr _in
            out <- liftIO $ pullOutBuffer zstr _out
            iter' <- lift $ enumPure1Chunk out iter
            idone iter' (Chunk remaining)
        Right True -> do
            (avail_in, avail_out) <- liftIO $ withZStream zstr $ \zptr -> do
                avail_in <- liftIO $ #{peek z_stream, avail_in} zptr
                avail_out <- liftIO $ #{peek z_stream, avail_out} zptr
                return (avail_in, avail_out) :: IO (CInt, CInt)
            case avail_out of
                0 -> do
                    out <- liftIO $ withZStream zstr $ \zptr ->
                        pullOutBuffer zstr _out
                    iter' <- lift $ enumPure1Chunk out iter
                    finish size run fin iter'
                _ -> do
                    _ <-  lift $ enumErr (toException IncorrectState) iter
                    throwErr $! toException IncorrectState

foreign import ccall unsafe deflateInit2_ :: Ptr ZStream -> CInt -> CInt
                                          -> CInt -> CInt -> CInt
                                          -> CString -> CInt -> IO CInt
foreign import ccall unsafe inflateInit2_ :: Ptr ZStream -> CInt
                                          -> CString -> CInt -> IO CInt
foreign import ccall unsafe inflate :: Ptr ZStream -> CInt -> IO CInt
foreign import ccall unsafe deflate :: Ptr ZStream -> CInt -> IO CInt
foreign import ccall unsafe "&deflateEnd"
                              deflateEnd :: FunPtr (Ptr ZStream -> IO ())
foreign import ccall unsafe "&inflateEnd"
                              inflateEnd :: FunPtr (Ptr ZStream -> IO ())

deflateInit2 :: Ptr ZStream -> CInt -> CInt -> CInt -> CInt -> CInt -> IO CInt
deflateInit2 s l m wB mL s'
    = withCString #{const_str ZLIB_VERSION} $ \v ->
        deflateInit2_ s l m wB mL s' v #{size z_stream}

inflateInit2 :: Ptr ZStream -> CInt -> IO CInt
inflateInit2 s wB
    = withCString #{const_str ZLIB_VERSION} $ \v ->
        inflateInit2_ s wB v #{size z_stream}

#ifdef DEBUG
deflate' :: ZStream -> CInt -> IO CInt
deflate' z f = withZStream z $ \p -> do
    IO.hPutStrLn stderr "About to run deflate"
    deflate p f

inflate' :: ZStream -> CInt -> IO CInt
inflate' z f = withZStream z $ \p -> do
    IO.hPutStrLn stderr "About to run inflate"
    inflate p f
#else
deflate' :: ZStream -> CInt -> IO CInt
deflate' z f = withZStream z $ \p -> deflate p f

inflate' :: ZStream -> CInt -> IO CInt
inflate' z f = withZStream z $ \p -> inflate p f
#endif

mkCompress :: Format -> CompressParams
           -> IO (Either ZLibParamsException Initial)
mkCompress frm cp
    = case convParam frm cp of
        Left err -> return $! Left err
        Right (c, m, b, l, s) -> do
            zstr <- mallocForeignPtrBytes #{size z_stream}
            withForeignPtr zstr $ \zptr -> do
                memset (castPtr zptr) 0 #{size z_stream}
                deflateInit2 zptr c m b l s `finally`
                    addForeignPtrFinalizer deflateEnd zstr
            return $! Right $! Initial $ ZStream zstr

mkDecompress :: Format -> DecompressParams
             -> IO (Either ZLibParamsException Initial)
mkDecompress frm cp@(DecompressParams wB _)
    = case fromWindowBits frm wB of
        Left err -> return $! Left err
        Right wB' -> do
            zstr <- mallocForeignPtrBytes #{size z_stream}
            withForeignPtr zstr $ \zptr -> do
                memset (castPtr zptr) 0 #{size z_stream}
                inflateInit2 zptr wB' `finally`
                    addForeignPtrFinalizer inflateEnd zstr
            return $! Right $! Initial $ ZStream zstr

-- User-related code

-- | Compress the input and send to inner iteratee.
enumDeflate :: MonadIO m
            => Format -- ^ Format of input
            -> CompressParams -- ^ Parameters of compression
            -> Enumeratee ByteString ByteString m a
enumDeflate f cp@(CompressParams _ _ _ _ _ size) iter = do
    cmp <- liftIO $ mkCompress f cp
    case cmp of
        Left err -> do
            _ <- lift $ enumErr err iter
            throwErr (toException err)
        Right init -> insertOut size deflate' init iter

-- | Decompress the input and send to inner iteratee. If there is end of
-- zlib stream it is left unprocessed.
enumInflate :: MonadIO m
            => Format
            -> DecompressParams
            -> Enumeratee ByteString ByteString m a
enumInflate f dp@(DecompressParams _ size) iter = do
    dcmp <- liftIO $ mkDecompress f dp
    case dcmp of
        Left err -> do
            _ <- lift $ enumErr err iter
            throwErr (toException err)
        Right init -> insertOut size inflate' init iter
