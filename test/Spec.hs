
import Test.Tasty
import Test.Tasty.HUnit

import Integration.Spec

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests = testGroup "Tests" [ integrationTests ]
