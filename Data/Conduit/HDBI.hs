{-# LANGUAGE
  BangPatterns
  #-}

module Data.Conduit.HDBI where

import Control.Monad.IO.Class
import Data.Conduit
import Database.HDBI

selectAll :: (Connection con, MonadResource m) => con -> Query -> [SqlValue] -> Source m [SqlValue]
selectAll con query params = statementSource fetch $ do
  st <- prepare con query
  execute st params
  return st

selectAllRows :: (Connection con, MonadResource m, FromRow a) => con -> Query -> [SqlValue] -> Source m a
selectAllRows con query params = statementSource fetchRow $ do
  st <- prepare con query
  execute st params
  return st

selectRawAll :: (Connection con, MonadResource m) => con -> Query -> Source m [SqlValue]
selectRawAll con query = statementSource fetch $ do
  st <- prepare con query
  executeRaw st
  return st

selectRawAllRows :: (Connection con, MonadResource m, FromRow a) => con -> Query -> Source m a
selectRawAllRows con query = statementSource fetchRow $ do
  st <- prepare con query
  executeRaw st
  return st

insertAllCount :: (Connection con, MonadResource m, Num count) => con -> Query -> Sink [SqlValue] m count
insertAllCount con query = statementSinkCount execute $ prepare con query


insertAllRowsCount :: (Connection con, MonadResource m, Num count, ToRow a) => con -> Query -> Sink a m count
insertAllRowsCount con query = statementSinkCount executeRow $ prepare con query


insertAll :: (Connection con, MonadResource m) => con -> Query -> Sink [SqlValue] m ()
insertAll con query = statementSink execute $ prepare con query


insertAllRows :: (Connection con, MonadResource m, ToRow a) => con -> Query -> Sink a m ()
insertAllRows con query = statementSink executeRow $ prepare con query


-- | Fetch the result of query using `fetchRow`. Statement must be executed.
statementSource :: (Statement stmt, MonadResource m) => (stmt -> IO (Maybe a)) -> IO stmt -> Source m a
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

-- | Execute query many times with given thread of parameters
statementSinkCount :: (Statement stmt, MonadResource m, Num count) => (stmt -> a -> IO ()) -> IO stmt -> Sink a m count
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

statementSink :: (Statement stmt, MonadResource m) => (stmt -> a -> IO ()) -> IO stmt -> Sink a m ()
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
