{-# LANGUAGE
  BangPatterns
, TypeFamilies
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

import Control.Monad.IO.Class
import Control.Monad.Trans.Resource
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

-- | fetch all results of query
selectAll :: (Connection con, MonadResource m)
             => con
             -> Query            -- query to execute
             -> [SqlValue]       -- query parameters
             -> Source m [SqlValue]
selectAll con query params = statementSource fetch $ do
  st <- prepare con query
  execute st params
  return st

-- | same as `selectAll` but reburn stream of `FromRow` instances
selectAllRows :: (Connection con, MonadResource m, FromRow a)
                 => con
                 -> Query
                 -> [SqlValue]
                 -> Source m a
selectAllRows con query params = statementSource fetchRow $ do
  st <- prepare con query
  execute st params
  return st

-- | same as `selectAll` but without query parameters
selectRawAll :: (Connection con, MonadResource m)
                => con
                -> Query
                -> Source m [SqlValue]
selectRawAll con query = statementSource fetch $ do
  st <- prepare con query
  executeRaw st
  return st

-- | same as `selectRawAll` but return stream of `FromRow` instances
selectRawAllRows :: (Connection con, MonadResource m, FromRow a) => con -> Query -> Source m a
selectRawAllRows con query = statementSource fetchRow $ do
  st <- prepare con query
  executeRaw st
  return st

-- | same as `insertAll` but also count executed rows
insertAllCount :: (Connection con, MonadResource m, Num count) => con -> Query -> Sink [SqlValue] m count
insertAllCount con query = statementSinkCount execute $ prepare con query

-- | same as `insertAllRows` but also count executed rows
insertAllRowsCount :: (Connection con, MonadResource m, Num count, ToRow a) => con -> Query -> Sink a m count
insertAllRowsCount con query = statementSinkCount executeRow $ prepare con query

-- | perform `execute` for each bunch of values
insertAll :: (Connection con, MonadResource m)
             => con
             -> Query
             -> Sink [SqlValue] m ()
insertAll con query = statementSink execute $ prepare con query

-- | perfor `executeRow` for each input row
insertAllRows :: (Connection con, MonadResource m, ToRow a)
                 => con
                 -> Query
                 -> Sink a m ()
insertAllRows con query = statementSink executeRow $ prepare con query


-- | Get all values from the statement until action return ''Just a''
statementSource :: (Statement stmt, MonadResource m)
                   => (stmt -> IO (Maybe a))  -- action to execute until it return
                                             -- Nothing
                   -> IO stmt               -- statement constructor
                   -> Source m a
statementSource getter stmt = bracketP
                              stmt
                              finish
                              statementSource'
  where
    statementSource' st = do
      row <- liftIO $ getter st
      case row of
        Nothing -> return ()
        Just r -> do
          yield r
          statementSource' st

-- | Execute action many times with given thread of values, return the count
-- of executions
statementSinkCount :: (Statement stmt, MonadResource m, Num count)
                      => (stmt -> a -> IO ()) -- action to execute each time
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
          liftIO $ do
            reset st
            putter st n
          statementSinkCount' (ac+1) st

-- | Same as `statementSinkCount` but without counting, just return ()
statementSink :: (Statement stmt, MonadResource m)
                 => (stmt -> a -> IO ())
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
          liftIO $ do
            reset st
            putter st n
          statementSink' st
