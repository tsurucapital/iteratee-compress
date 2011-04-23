{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Data.Iteratee.BZip
  (
    -- * Enumerators
    enumCompress,
    enumDecompress,
    -- * Exceptions
    BZipParamsException(..),
    BZipException(..),
    -- * Parameters
    CompressParams(..),
    defaultCompressParams,
    DecompressParams(..),
    defaultDecompressParams,
    BlockSize(..),
    WorkFactor(..)
  )
where
#include <bzlib.h>

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
data BZipParamsException
    = IncorrectBlockSize !Int
    -- ^ Incorrect block size was chosen
    | IncorrectWorkFactor !Int
    -- ^ Incorrect work size was chosen
    | IncorrectBufferSize !Int
    -- ^ Incorrect buffer size was chosen
    deriving (Eq,Typeable)

-- | Denotes error in compression and decompression
data BZipException
    = ConfigError
    -- ^ bzip2 library internal error
    | MemError
    -- ^ Memory allocation failed
    | DataError
    -- ^ Corrupted input
    | DataErrorMagic
    -- ^ Incorrect magic number
    | Unexpected !Int
    -- ^ Unknown or unexpected error
    | IncorrectState
    -- ^ Incorrect state - denotes error in library
    deriving (Eq,Typeable)

-- | Denotes the flush that can be sent to stream
data BZipFlush = BZipFlush
    deriving (Eq,Typeable)

instance Show BZipFlush where
    show BZipFlush = "bzlib: flush requested"

instance Exception BZipFlush

fromFlush :: BZipFlush -> CInt
fromFlush BZipFlush = #{const BZ_FLUSH}

instance Show BZipParamsException where
    show (IncorrectBlockSize size)
        = "bzlib: incorrect block size " ++ show size
    show (IncorrectWorkFactor wf)
        = "bzlib: incorrect work factor " ++ show wf
    show (IncorrectBufferSize size)
        = "bzlib: incorrect buffer size " ++ show size

instance Show BZipException where
    show ConfigError = "bzlib: library is not configure properly"
    show MemError = "bzlib: memory allocation failed"
    show DataError = "bzlib: input is corrupted"
    show DataErrorMagic = "bzlib: magic number does not match"
    show (Unexpected n) = "bzlib: unexpected error " ++ show n
    show IncorrectState = "bzlib: incorrect state"


instance Exception BZipParamsException
instance Exception BZipException 

newtype BZStream = BZStream (ForeignPtr BZStream)
withBZStream :: BZStream -> (Ptr BZStream -> IO a) -> IO a
withBZStream (BZStream fptr) = withForeignPtr fptr

mallocBZStream :: IO BZStream
mallocBZStream = BZStream <$> mallocForeignPtrBytes #{size bz_stream}

-- Following code is copied from Duncan Coutts bzlib haskell library version
-- 0.5.2.0 ((c) 2006-2008 Duncan Coutts, published on BSD licence) and adapted

-- | Set of parameters for compression. For sane defaults use
-- 'defaultCompressParams'
data CompressParams = CompressParams {
      compressBlockSize :: BlockSize,
      compressWorkFactor :: WorkFactor,
      -- | The size of output buffer. That is the size of 'Chunk's that will be
      -- emitted to inner iterator (except the last 'Chunk').
      compressBufferSize :: !Int
    }

defaultCompressParams
    = CompressParams DefaultBlockSize DefaultWorkFactor (8*1024)

-- | Set of parameters for decompression. For sane defaults see 
-- 'defaultDecompressParams'.
data DecompressParams = DecompressParams {
      decompressSaveMemory :: !Bool,
      -- | The size of output buffer. That is the size of 'Chunk's that will be
      -- emitted to inner iterator (except the last 'Chunk').
      decompressBufferSize :: !Int
    }

defaultDecompressParams = DecompressParams False (8*1024)

-- | The compression level specify the tradeoff between speed and compression.
data BlockSize
    = DefaultBlockSize
    -- ^ Default compression level set at 6.
    | BestSpeed
    -- ^ The fastest compression method (however less compression)
    | BestCompression
    -- ^ The best compression method (however slowest)
    | CompressionLevel !Int
    -- ^ Compression level set by number from 0 to 250

data WorkFactor
    = DefaultWorkFactor
    | BestSpeedWorkFactor
    | BestCompressionWorkFactor
    | WorkFactor !Int

fromBlockSize :: BlockSize -> Either BZipParamsException CInt
fromBlockSize DefaultBlockSize = Right $! 6
fromBlockSize BestSpeed = Right $! 1
fromBlockSize BestCompression = Right $! 9
fromBlockSize (CompressionLevel lvl)
    | lvl < 0 || lvl > 250 = Left $! IncorrectBlockSize $! fromIntegral lvl
    | otherwise = Right $! fromIntegral lvl

fromWorkFactor :: WorkFactor -> Either BZipParamsException CInt
fromWorkFactor DefaultWorkFactor = Right $! 0
fromWorkFactor BestSpeedWorkFactor = Right $! 1
fromWorkFactor BestCompressionWorkFactor = Right $! 250
fromWorkFactor (WorkFactor wf)
    | wf < 0 || wf > 250 = Left $! IncorrectWorkFactor $! fromIntegral wf
    | otherwise =  Right $! fromIntegral wf

fromErrno :: CInt -> Either BZipException Bool
fromErrno (#{const BZ_OK}) = Right $! True
fromErrno (#{const BZ_RUN_OK}) = Right $! True
fromErrno (#{const BZ_FLUSH_OK}) = Right $! True
fromErrno (#{const BZ_FINISH_OK}) = Right $! True
fromErrno (#{const BZ_STREAM_END}) = Right $! False
fromErrno (#{const BZ_CONFIG_ERROR}) = Left $! ConfigError
fromErrno (#{const BZ_MEM_ERROR}) = Left $! MemError
fromErrno (#{const BZ_DATA_ERROR}) = Left $! DataError
fromErrno (#{const BZ_DATA_ERROR_MAGIC}) = Left $! DataErrorMagic
fromErrno n = Left $! Unexpected $! fromIntegral n

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

newtype Initial = Initial BZStream
data EmptyIn = EmptyIn !BZStream !ByteString
data FullOut = FullOut !BZStream !ByteString
data Invalid = Invalid !BZStream !ByteString !ByteString
data Finishing = Finishing !BZStream !ByteString
data Flushing = Flushing !BZStream !BZipFlush !ByteString

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

dumpZStream :: BZStream -> IO ()
dumpZStream bzstr = withBZStream bzstr $ \bzptr -> do
    IO.hPutStr stderr $ "<<BZStream@"
    IO.hPutStr stderr $ (show bzptr)
    IO.hPutStr stderr . (" next_in=" ++) . show =<<
        (#{peek bz_stream, next_in} bzptr :: IO (Ptr CChar))
    IO.hPutStr stderr . (" avail_in=" ++) . show =<<
        (#{peek bz_stream, avail_in} bzptr :: IO CUInt)
    total_in_lo <- #{peek bz_stream, total_in_lo32} bzptr :: IO CUInt
    total_in_hi <- #{peek bz_stream, total_in_hi32} bzptr :: IO CUInt
    let total_in_lo' = fromIntegral total_in_lo
        total_in_hi' = fromIntegral total_in_hi `shiftL` 32
        total_in = total_in_lo' + total_in_hi' :: Int64
    IO.hPutStr stderr $ " total_out=" ++ show (total_in :: Int64)
    IO.hPutStr stderr . (" next_out=" ++) . show =<<
        (#{peek bz_stream, next_out} bzptr :: IO (Ptr CChar))
    IO.hPutStr stderr . (" avail_out=" ++) . show =<<
        (#{peek bz_stream, avail_out} bzptr :: IO CUInt)
    total_out_lo <- #{peek bz_stream, total_out_lo32} bzptr :: IO CUInt
    total_out_hi <- #{peek bz_stream, total_out_hi32} bzptr:: IO CUInt
    let total_out_lo' = fromIntegral total_out_lo
        total_out_hi' = fromIntegral total_out_hi `shiftL` 32
        total_out = total_out_lo' + total_out_hi'
    IO.hPutStr stderr $ " total_out=" ++ show (total_out :: Int64)
    IO.hPutStrLn stderr ">>"
#else
mkByteString :: MonadIO m => Int -> m ByteString
mkByteString s = liftIO $ create s (\_ -> return ())
#endif

putOutBuffer :: Int -> BZStream -> IO ByteString
putOutBuffer size bzstr = do
    _out <- mkByteString size
    withByteString _out $ \ptr len -> withBZStream bzstr $ \bzptr -> do
        #{poke bz_stream, next_out} bzptr ptr
        #{poke bz_stream, avail_out} bzptr len
    return _out

putInBuffer :: BZStream -> ByteString -> IO ()
putInBuffer bzstr _in
    = withByteString _in $ \ptr len -> withBZStream bzstr $ \bzptr -> do
        #{poke bz_stream, next_in} bzptr ptr
        #{poke bz_stream, avail_in} bzptr len

pullOutBuffer :: BZStream -> ByteString -> IO ByteString
pullOutBuffer bzstr _out = withByteString _out $ \ptr _ -> do
    next_out <- withBZStream bzstr $ \bzptr -> #{peek bz_stream, next_out} bzptr
    return $! BS.take (next_out `minusPtr` ptr) _out

pullInBuffer :: BZStream -> ByteString -> IO ByteString
pullInBuffer bzstr _in = withByteString _in $ \ptr _ -> do
    next_in <- withBZStream bzstr $ \bzptr -> #{peek bz_stream, next_in} bzptr
    return $! BS.drop (next_in `minusPtr` ptr) _in

insertOut :: MonadIO m
          => Int
          -> (BZStream -> CInt -> IO CInt)
          -> Initial
          -> Enumerator ByteString m a
insertOut size run (Initial bzstr) iter = return $! do
    _out <- liftIO $ putOutBuffer size bzstr
#ifdef DEBUG
    liftIO $ IO.hPutStrLn stderr $ "Inserted out buffer of size " ++ show size
#endif
    joinIM $ fill size run (EmptyIn bzstr _out) iter

fill :: MonadIO m
     => Int
     -> (BZStream -> CInt -> IO CInt)
     -> EmptyIn
     -> Enumerator ByteString m a
fill size run (EmptyIn bzstr _out) iter
    = let fill' (Chunk _in)
              | not (BS.null _in) = do
                  liftIO $ putInBuffer bzstr _in
#ifdef DEBUG
                  liftIO $ IO.hPutStrLn stderr $
                      "Inserted in buffer of size " ++ show (BS.length _in)
#endif
                  joinIM $ doRun size run (Invalid bzstr _in _out) iter
              | otherwise = fillI
          fill' (EOF Nothing) = do
              out <- liftIO $ pullOutBuffer bzstr _out 
              iter' <- lift $ enumPure1Chunk out iter
              joinIM $ finish size run (Finishing bzstr BS.empty) iter'
          fill' (EOF (Just err))
              = case fromException err of
                  Just err' ->
                      joinIM $ flush size run (Flushing bzstr err' _out) iter
                  Nothing -> throwRecoverableErr err fill'
#ifdef DEBUG
          fillI = do
              liftIO $ IO.hPutStrLn stderr $ "About to insert in buffer"
              liftI fill'
#else
          fillI = liftI fill'
#endif
      in return $! fillI

swapOut :: MonadIO m
        => Int
        -> (BZStream -> CInt -> IO CInt)
        -> FullOut
        -> Enumerator ByteString m a
swapOut size run (FullOut bzstr _in) iter = return $! do
    _out <- liftIO $ putOutBuffer size bzstr
#ifdef DEBUG
    liftIO $ IO.hPutStrLn stderr $ "Swapped out buffer of size " ++ show size
#endif
    joinIM $ doRun size run (Invalid bzstr _in _out) iter

doRun :: MonadIO m
      => Int
      -> (BZStream -> CInt -> IO CInt)
      -> Invalid
      -> Enumerator ByteString m a
doRun size run (Invalid bzstr _in _out) iter = return $! do
#ifdef DEBUG
    liftIO $ IO.hPutStrLn stderr $ "About to run"
    liftIO $ dumpZStream bzstr
#endif
    status <- liftIO $ run bzstr #{const BZ_RUN}
#ifdef DEBUG
    liftIO $ IO.hPutStrLn stderr $ "Runned"
#endif
    case fromErrno status of
        Left err -> joinIM $ enumErr err iter
        Right False -> do -- End of stream
            remaining <- liftIO $ pullInBuffer bzstr _in
            out <- liftIO $ pullOutBuffer bzstr _out
            iter' <- lift $ enumPure1Chunk out iter
            res <- lift $ tryRun iter'
            case res of
                Left err@(SomeException _) -> throwErr err
                Right x -> idone x (Chunk remaining)
        Right True -> do -- Continue
            (avail_in, avail_out) <- liftIO $ withBZStream bzstr $ \bzptr -> do
                avail_in <- liftIO $ #{peek bz_stream, avail_in} bzptr
                avail_out <- liftIO $ #{peek bz_stream, avail_out} bzptr
                return (avail_in, avail_out) :: IO (CInt, CInt)
            case avail_out of
                0 -> do
                    out <- liftIO $ pullOutBuffer bzstr _out
                    iter' <- lift $ enumPure1Chunk out iter
                    joinIM $ case avail_in of
                        0 -> insertOut size run (Initial bzstr) iter'
                        _ -> swapOut size run (FullOut bzstr _in) iter'
                _ -> joinIM $ case avail_in of
                    0 -> fill size run (EmptyIn bzstr _out) iter
                    _ -> enumErr IncorrectState iter

flush :: MonadIO m
      => Int
      -> (BZStream -> CInt -> IO CInt)
      -> Flushing
      -> Enumerator ByteString m a
flush size run fin@(Flushing bzstr _flush _out) iter = return $! do
    status <- liftIO $ run bzstr (fromFlush _flush)
    case fromErrno status of
        Left err -> joinIM $ enumErr err iter
        Right False -> do -- Finished
            out <- liftIO $ pullOutBuffer bzstr _out
            iter' <- lift $ enumPure1Chunk out iter
            res <- lift $ tryRun iter'
            case res of
                Left err@(SomeException _) -> throwErr err
                Right x -> idone x (Chunk BS.empty)
        Right True -> do
            (avail_in, avail_out) <- liftIO $ withBZStream bzstr $ \bzptr -> do
                avail_in <- liftIO $ #{peek bz_stream, avail_in} bzptr
                avail_out <- liftIO $ #{peek bz_stream, avail_out} bzptr
                return (avail_in, avail_out) :: IO (CInt, CInt)
            case avail_out of
                0 -> do
                    out <- liftIO $ pullOutBuffer bzstr _out
                    iter' <- lift $ enumPure1Chunk out iter
                    out' <- liftIO $ putOutBuffer size bzstr
                    joinIM $ flush size run (Flushing bzstr _flush out') iter'
                _ -> joinIM $ insertOut size run (Initial bzstr) iter

finish :: MonadIO m
       => Int
       -> (BZStream -> CInt -> IO CInt)
       -> Finishing
       -> Enumerator ByteString m a
finish size run fin@(Finishing bzstr _in) iter = return $! do
#ifdef DEBUG
    liftIO $ IO.hPutStrLn stderr $
        "Finishing with out buffer of size " ++ show size
#endif
    _out <- liftIO $ putOutBuffer size bzstr
    status <- liftIO $ run bzstr #{const BZ_FINISH}
    case fromErrno status of
        Left err -> joinIM $ enumErr err iter
        Right False -> do -- Finished
            remaining <- liftIO $ pullInBuffer bzstr _in
            out <- liftIO $ pullOutBuffer bzstr _out
            iter' <- lift $ enumPure1Chunk out iter
            res <- lift $ tryRun iter'
            case res of
                Left err@(SomeException _) -> throwErr err
                Right x -> idone x (Chunk remaining)
        Right True -> do
            (avail_in, avail_out) <- liftIO $ withBZStream bzstr $ \bzptr -> do
                avail_in <- liftIO $ #{peek bz_stream, avail_in} bzptr
                avail_out <- liftIO $ #{peek bz_stream, avail_out} bzptr
                return (avail_in, avail_out) :: IO (CInt, CInt)
            case avail_out of
                0 -> do
                    out <- liftIO $ withBZStream bzstr $ \bzptr ->
                        pullOutBuffer bzstr _out
                    iter' <- lift $ enumPure1Chunk out iter
                    joinIM $ finish size run fin iter'
                _ -> throwErr $! SomeException IncorrectState

foreign import ccall unsafe "BZ2_bzCompressInit"
                             compressInit :: Ptr BZStream -> CInt -> CInt
                                          -> CInt -> IO CInt
foreign import ccall unsafe "BZ2_bzDecompressInit"
                             decompressInit :: Ptr BZStream -> CInt -> CInt
                                            -> IO CInt
foreign import ccall unsafe "BZ2_bzCompress"
                             compress :: Ptr BZStream -> CInt -> IO CInt
foreign import ccall unsafe "BZ2_bzDecompress"
                             decompress :: Ptr BZStream -> IO CInt
foreign import ccall unsafe "&BZ2_bzCompressEnd"
                             compressEnd :: FunPtr (Ptr BZStream -> IO ())
foreign import ccall unsafe "&BZ2_bzDecompressEnd"
                             decompressEnd :: FunPtr (Ptr BZStream -> IO ())

#ifdef DEBUG
compress' :: BZStream -> CInt -> IO CInt
compress' bz f = withBZStream bz $ \p -> do
    IO.hPutStrLn stderr "About to run compress"
    compress p f

decompress' :: BZStream -> CInt -> IO CInt
decompress' bz _ = withBZStream bz $ \p -> do
    IO.hPutStrLn stderr "About to run decompress"
    decompress p

verboseLevel :: CInt
verboseLevel = 3
#else
compress' :: BZStream -> CInt -> IO CInt
compress' bz f = withBZStream z $ \p -> compress p f

decompress :: BZStream -> CInt -> IO CInt
decompress' bz f = withBZStream z $ \p -> decompress p f

verboseLevel :: CInt
verboseLevel = 0
#endif

mkCompress :: CompressParams -> IO (Either BZipParamsException Initial)
mkCompress (CompressParams blk wf _)
    = case fromBlockSize blk of
        Left err -> return $! Left $! err
        Right blk' -> case fromWorkFactor wf of
            Left err -> return $! Left $! err
            Right wf' -> do
                bzstr <- mallocForeignPtrBytes #{size bz_stream}
                withForeignPtr bzstr $ \bzptr -> do
                    memset (castPtr bzptr) 0 #{size bz_stream}
                    compressInit bzptr blk' verboseLevel wf' `finally`
                        addForeignPtrFinalizer compressEnd bzstr
                return $! Right $! Initial $ BZStream bzstr

mkDecompress :: DecompressParams -> IO (Either BZipParamsException Initial)
mkDecompress (DecompressParams small _) = do
    bzstr <- mallocForeignPtrBytes #{size bz_stream}
    withForeignPtr bzstr $ \bzptr -> do
        memset (castPtr bzptr) 0 #{size bz_stream}
        decompressInit bzptr verboseLevel (if small then 0 else 1) `finally`
            addForeignPtrFinalizer decompressEnd bzstr
    return $! Right $! Initial $ BZStream bzstr

-- User-related code

-- | Compress the input and send to inner iteratee.
enumCompress :: MonadIO m
             => CompressParams -- ^ Parameters of compression
             -> Enumerator ByteString m a
enumCompress cp@(CompressParams _ _ size) iter = do
    cmp <- liftIO $ mkCompress cp
    case cmp of
        Left err -> enumErr err iter
        Right init -> insertOut size compress' init iter

-- | Decompress the input and send to inner iteratee. If there is end of
-- zlib stream it is left unprocessed.
enumDecompress :: MonadIO m
               => DecompressParams
               -> Enumerator ByteString m a
enumDecompress dp@(DecompressParams _ size) iter = do
    dcmp <- liftIO $ mkDecompress dp
    case dcmp of
        Left err -> enumErr err iter
        Right init -> insertOut size decompress' init iter
