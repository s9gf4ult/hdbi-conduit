{-# LANGUAGE
  BangPatterns
, TypeFamilies
, ScopedTypeVariables
  #-}

module Data.Conduit.HDBI
       (
         -- * Conduit functions
         selectAll
       , selectAllRows
       , selectRawAll
       , selectRawAllRows
       , insertAll
       , insertAllRows
       , insertAllCount
       , insertAllRowsCount
       , insertTransAll
       , insertTransAllRows
         -- * Auxiliary conduit functions
       , statementSource
       , statementSink
       , statementSinkCount
         -- * ResourceT functions
       , allocConnection
       , allocStmt
       , executeStmt
       , executeStmtRow
       , executeStmtRaw
       ) where

import Control.Exception (try, throw, SomeException(..))
import Control.Monad (when)
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource
import Control.Monad.Trans.Class (lift)
import Data.Conduit
import Database.HDBI

allocConnection :: (Connection con, MonadResource m) => IO con -> m (ReleaseKey, con)
allocConnection con = allocate con disconnect

allocStmt :: (Statement stmt, MonadResource m) => IO stmt -> m (ReleaseKey, stmt)
allocStmt stmt = allocate stmt finish

executeStmt :: (Connection con, (ConnStatement con) ~ stmt, MonadResource m) => con -> Query -> [SqlValue] -> m (ReleaseKey, stmt)
executeStmt con query vals = do
  (key, stmt) <- allocStmt $ prepare con query
  liftIO $ execute stmt vals
  return (key, stmt)

executeStmtRow :: (Connection con, (ConnStatement con) ~ stmt, ToRow row, MonadResource m) => con -> Query -> row -> m (ReleaseKey, stmt)
executeStmtRow con query row = do
  (key, stmt) <- allocStmt $ prepare con query
  liftIO $ executeRow stmt row
  return (key, stmt)

executeStmtRaw :: (Connection con, (ConnStatement con) ~ stmt, MonadResource m) => con -> Query -> m (ReleaseKey, stmt)
executeStmtRaw con query = do
  (key, stmt) <- allocStmt $ prepare con query
  liftIO $ executeRaw stmt
  return (key, stmt)

execStmt :: (Connection con, (ConnStatement con) ~ stmt)
            => (stmt -> row -> IO ())
            -> con -> Query -> row
            -> IO stmt
execStmt exec con query params = do
  st <- prepare con query
  (r :: Either SomeException ()) <- try $ exec st params
  case r of
    Left e -> do
      finish st
      throw e
    Right _ -> return st

-- | fetch all results of query
selectAll :: (Connection con, MonadResource m)
             => con
             -> Query            -- query to execute
             -> [SqlValue]       -- query parameters
             -> Source m [SqlValue]
selectAll con query params = statementSource (liftIO . fetch)
                             $ execStmt execute con query params

-- | same as `selectAll` but reburn stream of `FromRow` instances
selectAllRows :: (Connection con, MonadResource m, FromRow a)
                 => con
                 -> Query
                 -> [SqlValue]
                 -> Source m a
selectAllRows con query params = statementSource (liftIO . fetchRow)
                                 $ execStmt execute con query params


-- | same as `selectAll` but without query parameters
selectRawAll :: (Connection con, MonadResource m)
                => con
                -> Query
                -> Source m [SqlValue]
selectRawAll con query = statementSource (liftIO . fetch)
                         $ execStmt execute con query []

-- | same as `selectRawAll` but return stream of `FromRow` instances
selectRawAllRows :: (Connection con, MonadResource m, FromRow a) => con -> Query -> Source m a
selectRawAllRows con query = statementSource (liftIO . fetchRow)
                             $ execStmt execute con query []


-- | same as `insertAll` but also count executed rows
insertAllCount :: (Connection con, MonadResource m, Num count) => con -> Query -> Sink [SqlValue] m count
insertAllCount con query = statementSinkCount valuePutter $ prepare con query


-- | same as `insertAllRows` but also count executed rows
insertAllRowsCount :: (Connection con, MonadResource m, Num count, ToRow a) => con -> Query -> Sink a m count
insertAllRowsCount con query = statementSinkCount rowPutter $ prepare con query

-- | perform `execute` for each bunch of values
insertAll :: (Connection con, MonadResource m)
             => con
             -> Query
             -> Sink [SqlValue] m ()
insertAll con query = statementSink valuePutter $ prepare con query

-- | perform `executeRow` for each input row
insertAllRows :: (Connection con, MonadResource m, ToRow a)
                 => con
                 -> Query
                 -> Sink a m ()
insertAllRows con query = statementSink rowPutter $ prepare con query
  where
    putter st val = liftIO $ do
      reset st
      executeRow st val

-- | Execute query on each (Chunk [SqlValue]) and commit on each Flush
insertTransAll :: (Connection con, MonadResource m)
                  => con
                  -> Query
                  -> Sink (Flush [SqlValue]) m ()
insertTransAll con query = statementSinkTrans con valuePutter $ prepare con query

-- | Execute query on each (Chunk row) and commit on each Flush
insertTransAllRows :: (Connection con, MonadResource m, ToRow a)
                      => con
                      -> Query
                      -> Sink (Flush a) m ()
insertTransAllRows con query = statementSinkTrans con rowPutter $ prepare con query


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

-- | Execute each chunk with putter function and commit transaction on each flush
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


valuePutter :: (MonadIO m, Statement stmt)
               => stmt
               -> [SqlValue]
               -> m ()
valuePutter st vals = liftIO $ do
  reset st
  execute st vals

rowPutter :: (MonadIO m, Statement stmt, ToRow row)
               => stmt
               -> row
               -> m ()
rowPutter st row = liftIO $ do
  reset st
  executeRow st row
