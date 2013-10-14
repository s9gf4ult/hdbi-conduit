{-# LANGUAGE
  BangPatterns
, TypeFamilies
, ScopedTypeVariables
  #-}

module Data.Conduit.HDBI
       (
         -- * Conduit functions
         selectAll
       , insertAll
       , insertAllCount
       , insertAllTrans
         -- * Auxiliary conduit functions
       , statementSource
       , statementSink
       , statementSinkCount
       , statementSinkTrans
         -- * ResourceT functions
       , allocConnection
       , allocStmt
       , executeStmt
         -- * Stream typing helpers
       , asSqlVals
       , asThisType
       ) where

import Control.Exception (try, throw, SomeException(..))
import Control.Monad (when)
import Control.Monad.IO.Class
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Resource
import Data.Conduit
import Database.HDBI
import qualified Data.Conduit.List as L

allocConnection :: (Connection con, MonadResource m) => IO con -> m (ReleaseKey, con)
allocConnection con = allocate con disconnect

allocStmt :: (Statement stmt, MonadResource m) => IO stmt -> m (ReleaseKey, stmt)
allocStmt stmt = allocate stmt finish

executeStmt :: (Connection con, (ConnStatement con) ~ stmt, ToRow row, MonadResource m)
               => con -> Query -> row -> m (ReleaseKey, stmt)
executeStmt con query row = do
  (key, stmt) <- allocStmt $ prepare con query
  liftIO $ execute stmt row
  return (key, stmt)


-- | Execute query and stream result
selectAll :: (Connection con, MonadResource m, FromRow row, ToRow params)
             => con
             -> Query
             -> params
             -> Source m row
selectAll con query params = statementSource (liftIO . fetch) execStmt
  where
    execStmt = do
      st <- prepare con query
      (r :: Either SomeException ()) <- try $ execute st params
      case r of
        Left e -> do
          finish st
          throw e
        Right _ -> return st

-- | same as `insertAll` but also count executed rows
insertAllCount :: (Connection con, MonadResource m, Num count, ToRow a) => con -> Query -> Sink a m count
insertAllCount con query = statementSinkCount rowPutter $ prepare con query


-- | perform `executeRow` for each input row
insertAll :: (Connection con, MonadResource m, ToRow a)
             => con
             -> Query
             -> Sink a m ()
insertAll con query = statementSink rowPutter $ prepare con query

-- | Execute query on each (Chunk row) and commit on each Flush. The last query
-- is always commit, so be carefull.
insertAllTrans :: (Connection con, MonadResource m, ToRow a)
                  => con
                  -> Query
                  -> Sink (Flush a) m ()
insertAllTrans con query = statementSinkTrans con rowPutter $ prepare con query


-- | Get all values from the statement until action return ''Just a''
statementSource :: (Statement stmt, MonadResource m)
                   => (stmt -> m (Maybe a))  -- action to execute until it return
                                             -- Nothing
                   -> IO stmt               -- statement constructor
                   -> Source m a
statementSource getter stmt = bracketP
                              stmt
                              finish
                              statementSource'
  where
    statementSource' st = do
      row <- lift $ getter st
      case row of
        Nothing -> return ()
        Just r -> do
          yield r
          statementSource' st

-- | Execute action many times with given thread of values, return the count
-- of executions
statementSinkCount :: (Statement stmt, MonadResource m, Num count)
                      => (stmt -> a -> m ()) -- action to execute each time
                      -> IO stmt            -- statement constructor
                      -> Sink a m count
statementSinkCount putter stmt = bracketP
                                 stmt
                                 finish
                                 $ statementSinkCount' 0
  where
    statementSinkCount' !ac st = do
      next <- await
      case next of
        Nothing -> return ac
        Just n -> do
          lift $ putter st n
          statementSinkCount' (ac+1) st

-- | Same as `statementSinkCount` but without counting, just return ()
statementSink :: (Statement stmt, MonadResource m)
                 => (stmt -> a -> m ())
                 -> IO stmt
                 -> Sink a m ()
statementSink putter stmt = bracketP
                            stmt
                            finish
                            statementSink'
  where
    statementSink' st = do
      next <- await
      case next of
        Nothing -> return ()
        Just n -> do
          lift $ putter st n
          statementSink' st

-- | Execute each chunk with putter function and commit transaction on each
-- flush. The last action is always commit.
statementSinkTrans :: (Connection con, (ConnStatement con) ~ stmt, MonadResource m, MonadIO m)
                      => con
                      -> (stmt -> a -> m ())
                      -> IO stmt
                      -> Sink (Flush a) m ()
statementSinkTrans con putter stmt = do
  intrans <- liftIO $ inTransaction con
  bracketP
    stmt
    finish
    $ statementSinkTrans' intrans
  where
    statementSinkTrans' intrans st = do
      next <- await
      case next of
        Nothing -> do
          when intrans $ liftIO $ commit con
          return ()
        Just n -> case n of
          Flush -> do
            when intrans $ liftIO $ commit con
            statementSinkTrans' False st
          Chunk val -> do
            when (not intrans) $ liftIO $ begin con
            lift $ putter st val
            statementSinkTrans' True st

-- | The behaviour is the same as `groupBy` function. Each time when prefix
-- return False the conduit yields Flush.
flushBy :: (Monad m) => (a -> a -> Bool) -> Conduit a m (Flush a)
flushBy pref = flushBy' Nothing
  where
    flushBy' !last = case last of
      Nothing -> do
        n <- await
        case n of
          Nothing -> return ()
          Just x -> do
            yield $ Chunk x
            flushBy' $ Just x
      Just l -> do
        n <- await
        case n of
          Nothing -> return ()
          Just x -> do
            when (not $ pref l x) $ yield Flush
            yield $ Chunk x
            flushBy' $ Just x

-- | separate each `i` chunks with Flush
flushAt :: (Monad m, Integral i) => i -> Conduit a m (Flush a)
flushAt cnt = flushAt' c
  where
    c = max 1 cnt
    flushAt' 0 = do
      yield Flush
      flushAt' c
    flushAt' !count = do
      n <- await
      case n of
        Nothing -> return ()
        Just x -> do
          yield $ Chunk x
          flushAt' $ count - 1


rowPutter :: (MonadIO m, Statement stmt, ToRow row)
               => stmt
               -> row
               -> m ()
rowPutter st row = liftIO $ do
  reset st
  execute st row

-- | Function to fuse when no data convertion is needed
asSqlVals :: (Monad m) => Conduit [SqlValue] m [SqlValue]
asSqlVals = L.map id

-- | To specify actual stream type by fusing with it. The value of argument is
-- not used
asThisType :: (Monad m) => a -> Conduit a m a
asThisType _ = L.map id
