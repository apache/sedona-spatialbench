// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Consistence test suite to test the `IntoIterator` trait implementation
//! for the generators in the `spatialbench` crate.

use spatialbench::generators::{
    BuildingGenerator, CustomerGenerator, DriverGenerator, TripGenerator, VehicleGenerator,
};

struct TestIntoIterator<G>
where
    G: IntoIterator,
    G::Item: std::fmt::Display,
{
    generator: Option<G>,
}

impl<G> TestIntoIterator<G>
where
    G: IntoIterator,
    G::Item: std::fmt::Display,
{
    fn new(generator: G) -> Self {
        Self {
            generator: Some(generator),
        }
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_string_vec(&mut self, take_num: i32) -> Vec<String> {
        if let Some(generator) = self.generator.take() {
            generator
                .into_iter()
                .take(take_num as usize)
                .map(|item| item.to_string())
                .collect()
        } else {
            vec![]
        }
    }
}

#[test]
fn test_trip_into_iter() {
    {
        assert_eq!(
            TestIntoIterator::new(TripGenerator::new(0.01, 1, 1))
                .to_string_vec(5)
                .len(),
            5
        );
    }
    {
        let trip = TripGenerator::new(0.01, 1, 1);
        assert_eq!(TestIntoIterator::new(trip).to_string_vec(5).len(), 5);
    }
}

#[test]
fn test_customer_into_iter() {
    {
        assert_eq!(
            TestIntoIterator::new(CustomerGenerator::new(0.01, 1, 1))
                .to_string_vec(5)
                .len(),
            5
        );
    }
    {
        let customer = CustomerGenerator::new(0.01, 1, 1);
        assert_eq!(TestIntoIterator::new(customer).to_string_vec(5).len(), 5);
    }
}

#[test]
fn test_driver_into_iter() {
    {
        assert_eq!(
            TestIntoIterator::new(DriverGenerator::new(0.01, 1, 1))
                .to_string_vec(5)
                .len(),
            5
        );
    }
    {
        let driver = DriverGenerator::new(0.01, 1, 1);
        assert_eq!(TestIntoIterator::new(driver).to_string_vec(5).len(), 5);
    }
}

#[test]
fn test_vehicle_into_iter() {
    {
        assert_eq!(
            TestIntoIterator::new(VehicleGenerator::new(0.01, 1, 1))
                .to_string_vec(5)
                .len(),
            1
        );
    }
    {
        let vehicle = VehicleGenerator::new(0.01, 1, 1);
        assert_eq!(TestIntoIterator::new(vehicle).to_string_vec(5).len(), 1);
    }
}

#[test]
fn test_building_into_iter() {
    {
        let gen = BuildingGenerator::new(1.0, 1, 1);
        assert_eq!(TestIntoIterator::new(&gen).to_string_vec(5).len(), 5);
    }
    {
        let building = BuildingGenerator::new(1.0, 1, 1);
        assert_eq!(TestIntoIterator::new(&building).to_string_vec(5).len(), 5);
    }
}
