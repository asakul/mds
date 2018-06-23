
module Integration.Spec (
  integrationTests
) where

import Test.Tasty
import Test.Tasty.HUnit

import Integration.Database

integrationTests = testGroup "Integration tests" [ testDatabase ]

