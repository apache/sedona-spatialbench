//! Consistence test suite to test the `IntoIterator` trait implementation
//! for the generators in the `tpchgen` crate.

use tpchgen::generators::{
    BuildingGenerator, CustomerGenerator, DriverGenerator, TripGenerator, VehicleGenerator,
    ZoneGenerator,
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
fn test_zone_into_iter() {
    {
        assert_eq!(
            TestIntoIterator::new(ZoneGenerator::new(0.001, 1, 1))
                .to_string_vec(5)
                .len(),
            5
        );
    }
    {
        let zone = ZoneGenerator::new(0.001, 1, 1);
        assert_eq!(TestIntoIterator::new(zone).to_string_vec(5).len(), 5);
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
