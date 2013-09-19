{-# LANGUAGE
  BangPatterns
  #-}

module Data.Conduit.HDBI where

import Control.Monad.IO.Class
import Control.Monad
import Data.Conduit
import Database.HDBI


-- | fetch sources 
statementSource :: (Statement stmt, MonadResource m) => stmt -> Source m [SqlValue]
statementSource stmt = bracketP
                       (return stmt)
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


statementSink :: (Statement stmt, MonadResource m, Num count) => stmt -> Sink [SqlValue] m count
statementSink stmt = bracketP
                     (return stmt)
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
            
             
    
      
      
