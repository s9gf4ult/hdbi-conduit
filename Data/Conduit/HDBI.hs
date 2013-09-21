{-# LANGUAGE
  BangPatterns
  #-}

module Data.Conduit.HDBI where

import Control.Monad.IO.Class
import Control.Monad
import Data.Conduit
import Database.HDBI
-- import Debug.Trace

selectAll :: (Connection con, MonadResource m) => con -> Query -> [SqlValue] -> Source m [SqlValue]
selectAll con query params = statementSource $ do
  st <- prepare con query
  execute st params
  return st

selectRawAll :: (Connection con, MonadResource m) => con -> Query -> Source m [SqlValue]
selectRawAll con query = statementSource $ do
  st <- prepare con query
  executeRaw st
  return st

insertAll :: (Connection con, MonadResource m, Num count) => con -> Query -> Sink [SqlValue] m count
insertAll con query = statementSink $ prepare con query


-- | Fetch the result of query using `fetchRow`. Statement must be executed.
statementSource :: (Statement stmt, MonadResource m) => IO stmt -> Source m [SqlValue]
statementSource stmt = bracketP
                       stmt
                       finish
                       statementSource'
  where
    statementSource' st = do
      row <- liftIO $ fetchRow st
      case row of
        Nothing -> return ()
        Just r -> do
          yield r
          statementSource' st

-- | Execute query many times with given thread of parameters
statementSink :: (Statement stmt, MonadResource m, Num count) => IO stmt -> Sink [SqlValue] m count
statementSink stmt = bracketP
                     stmt
                     finish
                     $ statementSink' 0
  where
    statementSink' !ac st = do
      next <- await
      case next of
        Nothing -> return ac
        Just n -> do
          liftIO $ do
            state <- statementStatus st
            when (StatementNew /= state) $ reset st
            execute st n
          statementSink' (ac+1) st
