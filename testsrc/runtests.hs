{-# LANGUAGE
  OverloadedStrings
, BangPatterns
, ScopedTypeVariables
  #-}

module RunTests where

import Control.Exception
import Data.Conduit
import Data.List (foldl')
import Data.Monoid ((<>))
import Data.Conduit.HDBI
import Database.HDBI
import Database.HDBI.SQlite
import Test.Framework
import Test.Framework.Providers.QuickCheck2
import Test.QuickCheck
import Test.QuickCheck.Assertions
import qualified Data.Conduit.List as L
import qualified Data.Conduit.Util as U
import qualified Test.QuickCheck.Monadic as M

createTables :: SQliteConnection -> IO ()
createTables c = do
  runRaw c "create table values1(val1 integer, val2 integer)"
  runRaw c "create table values2(val1 integer, val2 integer)"
  runRaw c "create table values3(val1 integer, val2 integer)"


allTests :: SQliteConnection -> Test
allTests c = testGroup "All tests"
             [ testProperty "Insert + fold" $ insertFold c
             , testProperty "Insert + copy" $ insertCopy c
             , testProperty "Insert + copy + sum" $ insertCopySum c]

sumPairs :: (Num a, Num b) => (a, b) -> (a, b) -> (a, b)
sumPairs (!a, !b) (!x, !y) = (a+x, b+y)


-- insertTransactional :: SQliteConnection -> [Flush (Integer, Integer)] -> Property
-- insertTransactional con vals = M.monadicIO $ do
--   res <- M.run $ do
--     runRaw c "delete from values1"
--     runResourceT
--       $ L.sourceList vals
--       $$ insertTransAllRows c "insert into values1 (val1, val2) values (?,?)"
  
--   M.stop $ 

insertFold :: SQliteConnection -> [(Integer, Integer)] -> Property
insertFold c vals = M.monadicIO $ do
  res <- M.run $ withTransaction c $ do
    runRaw c "delete from values1"
    runMany c "insert into values1(val1, val2) values (?,?)" vals
    runResourceT
      $ selectAll c "select val1, val2 from values1" ()
      $$ L.fold sumPairs (0 :: Integer, 0 :: Integer)
  M.stop $ res ?== (foldl' sumPairs (0, 0) vals)

insertCopy :: SQliteConnection -> [(Integer, Integer)] -> Property
insertCopy c vals = M.monadicIO $ do
  res <- M.run $ withTransaction c $ do
    runRaw c "delete from values1"
    runRaw c "delete from values2"
    runResourceT
      $ L.sourceList vals
      $$ insertAll c "insert into values1(val1, val2) values (?,?)"
    runResourceT
      $ selectAll c "select val1, val2 from values1" () $= asThisType (undefined :: (Int, Int))
      $$ insertAllCount c "insert into values2(val1, val2) values (?,?)"
  M.stop $ res == (length vals)

insertCopySum :: SQliteConnection -> [(Integer, Integer)] -> Property
insertCopySum c vals = M.monadicIO $ do
  res <- M.run $ withTransaction c $ do
    mapM_ (runRaw c . ("delete from " <>)) ["values1",
                                            "values2",
                                            "values3"]
    runResourceT
      $ L.sourceList vals
      $$ insertAll c "insert into values1(val1, val2) values (?,?)"
    runResourceT
      $ selectAll c "select val1, val2 from values1" () $= asSqlVals
      $$ insertAll c "insert into values2(val1, val2) values (?,?)"
    runResourceT
      $ (U.zip
         (selectAll c "select val1, val2 from values1" ())
         (selectAll c "select val1, val2 from values2" ()))
      $= L.map (\(a, b :: (Integer, Integer)) -> sumPairs a b)
      $$ insertAll c "insert into values3(val1, val2) values (?,?)"
    runResourceT
      $ selectAll c "select val1, val2 from values3" ()
      $$ L.fold sumPairs (0 :: Integer, 0 :: Integer)
  let (a, b) = foldl' sumPairs (0, 0) vals
  M.stop $ (a*2, b*2) ==? res

main :: IO ()
main = bracket (connectSqlite3 ":memory:") disconnect $ \c -> do
  createTables c
  defaultMain [allTests c]
