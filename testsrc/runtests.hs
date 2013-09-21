{-# LANGUAGE
  OverloadedStrings
  #-}

module RunTests where

import Control.Exception
import Data.Conduit
import Data.Conduit.HDBI
import Database.HDBI
import Database.HDBI.SQlite
import Test.Framework
import Test.Framework.Providers.QuickCheck2
import Test.QuickCheck
import Test.QuickCheck.Assertions
import qualified Data.Conduit.List as L
import qualified Test.QuickCheck.Monadic as M

createTables :: SQliteConnection -> IO ()
createTables c = do
  runRaw c "create table values1(val1 integer, val2 integer)"
  runRaw c "create table values2(val1 integer, val2 integer)"

allTests :: SQliteConnection -> Test
allTests c = testGroup "All tests"
             [ testProperty "Insert + fold" $ insertFold c
             , testProperty "Insert + copy" $ insertCopy c]

sumInts :: (Monad m, Num a, Num b, FromSql a, FromSql b) => Consumer [SqlValue] m (a, b)
sumInts = L.fold (\(a, b) [x, y] -> (a + fromSql x, b + fromSql y)) (0, 0)

insertFold :: SQliteConnection -> [(Integer, Integer)] -> Property 
insertFold c vals = M.monadicIO $ do
  res <- M.run $ do
    runRaw c "delete from values1"
    runMany c "insert into values1(val1, val2) values (?,?)" $ map (\(a, b) -> [toSql a, toSql b]) vals
    runResourceT $ selectRawAll c "select val1, val2 from values1" $$ sumInts
  M.stop $ res ?== mustres
  where
    mustres = foldl (\(a, b) (x, y) -> (a+x, b+y)) (0, 0) vals

insertCopy :: SQliteConnection -> [(Integer, Integer)] -> Property
insertCopy c vals = M.monadicIO $ do
  res <- M.run $ do
    runRaw c "delete from values1"
    runRaw c "delete from values2"
    runResourceT
      $ L.sourceList (map (\(a, b) -> [toSql a, toSql b]) vals)
      $$ insertAll c "insert into values1(val1, val2) values (?,?)"
    runResourceT
      $ selectRawAll c "select val1, val2 from values1"
      $$ insertAll c "insert into values1(val1, val2) values (?,?)"
  M.stop $ res == (length vals)
  

main :: IO ()
main = bracket (connectSqlite3 ":memory:") disconnect $ \c -> do
  createTables c
  defaultMain [allTests c]
  
         
         
  
  
