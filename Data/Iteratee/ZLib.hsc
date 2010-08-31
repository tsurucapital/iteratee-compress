{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Data.Iteratee.ZLib
  (   
    ZLibParamsException(..),
    ZLibException(..),
    CompressParams(..),
    DecompressParams(..),
    Format(..),
    enumInflate,
    enumDeflate,
  )
where
#include <zlib.h>

import Codec.Compression.Zlib.Internal hiding (StreamError, DataError)
import Control.Applicative
import Control.Exception
import Control.Monad.Trans
import Data.ByteString as BS
import Data.ByteString.Internal
import Data.Iteratee
import Data.Typeable
import Foreign
import Foreign.C

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
    -- ^ Buffer error - denotes the library error
--    | File Error
    | StreamError
    -- ^ State of steam inconsistent
    | DataError
    -- ^ Input data corrupted
    | MemoryError
    -- ^ Not enought memory
    | VersionError
    -- ^ Version error
    | Unexpected !CInt
    -- ^ Unexpected or unknown error - please report as bug
    | IncorrectState
    -- ^ Incorrect state - denotes error in library
    deriving (Eq,Typeable)

instance Show ZLibParamsException where
    show (IncorrectCompressionLevel lvl)
        = "zlib: incorrect compression level " ++ show lvl
    show (IncorrectWindowBits lvl)
        = "zlib: incorrect window bits " ++ show lvl
    show (IncorrectMemoryLevel lvl)
        = "zlib: incorrect memory lvele " ++ show lvl

instance Show ZLibException where
    show NeedDictionary = "zlib: needs dictionary"
    show BufferError = "zlib: no progress is possible (internall error)"
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
-- In following code we go through 6 states. Some of the operations are
-- 'deterministic' like 'insertOut' and some of them depends on input ('fill')
-- or library call.
--
--              insertOut                fill[1]
--  (Initial) -------------> (EmptyIn) -----------> (Finishing)
--         ^                    ^ |                    |
--         |             run[2] | |                    |
--         |    run[1]          | |                    |
--         \------------------\ | | fill[0]            | finish
--                            | | |                    |
--                            | | |                    |
--               swapOut      | | v                    v
--  (FullOut) -------------> (Invalid)              (Finished)
--            <------------
--               run[0]
--
-- Initial: Initial state, both buffers are empty
-- EmptyIn: Empty in buffer, out waits untill filled
-- FullOut: Out was filled and sent. In was not entirely read
-- Invalid[1]: Both buffers non-empty
-- Finishing: There is no more in data and in buffer is empty. Waits till
--    all outs was sent.
-- Finished: Operation finished
-- 
-- [1] Named for 'historical' reasons

mkByteString :: MonadIO m => Int -> m ByteString
mkByteString s = liftIO $ create s (\_ -> return ())

newtype Initial = Initial ZStream
data EmptyIn = EmptyIn !ZStream !ByteString
data FullOut = FullOut !ZStream !ByteString
data Invalid = Invalid !ZStream !ByteString !ByteString
data Finishing = Finishing !ZStream !ByteString

withByteString :: ByteString -> (Ptr Word8 -> Int -> IO a) -> IO a
withByteString (PS ptr off len) f
    = withForeignPtr ptr (\ptr' -> f (ptr' `plusPtr` off) len)

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
          -> Enumerator ByteString m a
insertOut size run (Initial zstr) iter = return $! do
    _out <- liftIO $ putOutBuffer size zstr
    joinIM $ fill size run (EmptyIn zstr _out) iter

fill :: MonadIO m
     => Int
     -> (ZStream -> CInt -> IO CInt)
     -> EmptyIn
     -> Enumerator ByteString m a
fill size run (EmptyIn zstr _out) iter
    = let fillI = liftI fill'
          fill' (Chunk _in)
              | BS.null _in = do
                  liftIO $ putInBuffer zstr _in
                  joinIM $ doRun size run (Invalid zstr _in _out) iter
              | otherwise = fillI
          fill' (EOF Nothing)
              = joinIM $ finish size run (Finishing zstr BS.empty) iter
          fill' (EOF (Just err)) = throwRecoverableErr err fill'
      in return $! fillI

swapOut :: MonadIO m
        => Int
        -> (ZStream -> CInt -> IO CInt)
        -> FullOut
        -> Enumerator ByteString m a
swapOut size run (FullOut zstr _in) iter = return $! do
    _out <- liftIO $ putOutBuffer size zstr
    joinIM $ doRun size run (Invalid zstr _in _out) iter

doRun :: MonadIO m
      => Int
      -> (ZStream -> CInt -> IO CInt)
      -> Invalid
      -> Enumerator ByteString m a
doRun size run (Invalid zstr _in _out) iter = return $! do
    status <- liftIO $ run zstr #{const Z_NO_FLUSH}
    case fromErrno status of
        Left err -> joinIM $ enumErr err iter
        Right False -> do -- End of stream
            remaining <- liftIO $ pullInBuffer zstr _in
            out <- liftIO $ pullOutBuffer zstr _out
            iter' <- lift $ enumPure1Chunk out iter
            res <- lift $ tryRun iter'
            case res of
                Left err@(SomeException _) -> throwErr err
                Right x -> idone x (Chunk remaining)
        Right True -> do -- Continue
            (avail_in, avail_out) <- liftIO $ withZStream zstr $ \zptr -> do
                avail_in <- liftIO $ #{peek z_stream, avail_in} zptr
                avail_out <- liftIO $ #{peek z_stream, avail_out} zptr
                return (avail_in, avail_out) :: IO (CInt, CInt)
            case avail_out of
                0 -> do
                    out <- liftIO $ pullOutBuffer zstr _out
                    iter' <- lift $ enumPure1Chunk out iter
                    joinIM $ case avail_in of
                        0 -> insertOut size run (Initial zstr) iter'
                        _ -> swapOut size run (FullOut zstr _in) iter'
                _ -> joinIM $ case avail_in of
                    0 -> fill size run (EmptyIn zstr _out) iter
                    _ -> enumErr IncorrectState iter

finish :: MonadIO m
       => Int
       -> (ZStream -> CInt -> IO CInt)
       -> Finishing
       -> Enumerator ByteString m a
finish size run fin@(Finishing zstr _in) iter = return $! do
    _out <- liftIO $ putOutBuffer size zstr
    status <- liftIO $ run zstr #{const Z_FINISH}
    case fromErrno status of
        Left err -> joinIM $ enumErr err iter
        Right False -> do -- Finished
            remaining <- liftIO $ pullInBuffer zstr _in
            out <- liftIO $ pullOutBuffer zstr _out
            iter' <- lift $ enumPure1Chunk out iter
            res <- lift $ tryRun iter'
            case res of
                Left err@(SomeException _) -> throwErr err
                Right x -> idone x (Chunk remaining)
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
                    joinIM $ finish size run fin iter'
                _ -> throwErr $! SomeException IncorrectState

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

deflate' :: ZStream -> CInt -> IO CInt
deflate' z f = withZStream z $ \p -> deflate p f

inflate' :: ZStream -> CInt -> IO CInt
inflate' z f = withZStream z $ \p -> inflate p f

mkCompress :: Format -> CompressParams
           -> IO (Either ZLibParamsException Initial)
mkCompress frm cp
    = case convParam frm cp of
        Left err -> return $! Left err
        Right (c, m, b, l, s) -> do
            zstr <- mallocForeignPtrBytes #{size z_stream}
            withForeignPtr zstr $ \zptr -> do
                memset (castPtr zptr) 0 #{size z_stream}
                deflateInit2 zptr c m b l s
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
                inflateInit2 zptr wB'
                addForeignPtrFinalizer inflateEnd zstr
            return $! Right $! Initial $ ZStream zstr

-- User-releted code

-- | Compress the input and send to inner iteratee.
enumDeflate :: MonadIO m
            => Format -- ^ Format of input
            -> CompressParams -- ^ Parameters of compression
            -> Enumerator ByteString m a
enumDeflate f cp@(CompressParams _ _ _ _ _ size) iter = do
    cmp <- liftIO $ mkCompress f cp
    case cmp of
        Left err -> enumErr err iter
        Right init -> insertOut size deflate' init iter

-- | Decompress the input and send to inner iteratee. If there is end of
-- zlib stream it is left unprocessed.
enumInflate :: MonadIO m
            => Format
            -> DecompressParams
            -> Enumerator ByteString m a
enumInflate f dp@(DecompressParams _ size) iter = do
    dcmp <- liftIO $ mkDecompress f dp
    case dcmp of
        Left err -> enumErr err iter
        Right init -> insertOut size deflate' init iter
