//! Generators for each TPC-H Tables
use crate::dates;
use crate::dates::{GenerateUtils, TPCHDate};
use crate::decimal::TPCHDecimal;
use crate::distribution::Distribution;
use crate::distribution::Distributions;
use crate::random::RandomPhoneNumber;
use crate::random::RowRandomInt;
use crate::random::{PhoneNumberInstance, RandomBoundedLong, StringSequenceInstance};
use crate::random::{RandomAlphaNumeric, RandomAlphaNumericInstance};
use crate::random::{RandomBoundedInt, RandomString, RandomStringSequence, RandomText};
use crate::spider::{spider_seed_for_index, SpiderGenerator};
use crate::spider_defaults::SpiderDefaults;
use crate::spider_overrides;
use crate::text::TextPool;
use duckdb::Connection;
use geo::Geometry;
use geo::Point;
use geozero::{wkb::Wkb, ToGeo};
use log::{debug, error, info};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::convert::TryInto;
use std::fmt;
use std::fmt::Display;
use std::time::Instant;

/// A Vehicle Manufacturer, formatted as `"Manufacturer#<n>"`
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VehicleManufacturerName(i32);

impl VehicleManufacturerName {
    pub fn new(value: i32) -> Self {
        VehicleManufacturerName(value)
    }
}

impl Display for VehicleManufacturerName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Manufacturer#{}", self.0)
    }
}

/// A Vehicle brand name, formatted as `"Brand#<n>"`
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VehicleBrandName(i32);

impl VehicleBrandName {
    pub fn new(value: i32) -> Self {
        VehicleBrandName(value)
    }
}

impl Display for VehicleBrandName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Brand#{}", self.0)
    }
}

/// The VEHICLE table
///
/// The Display trait is implemented to format the line item data as a string
/// in the default TPC-H 'tbl' format.
///
/// ```text
/// 1|goldenrod lavender spring chocolate lace|Manufacturer#1|Brand#13|PROMO BURNISHED COPPER|7|JUMBO PKG|901.00|ly. slyly ironi|
/// 2|blush thistle blue yellow saddle|Manufacturer#1|Brand#13|LARGE BRUSHED BRASS|1|LG CASE|902.00|lar accounts amo|
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Vehicle<'a> {
    /// Primary key
    pub v_vehiclekey: i64,
    /// Vehicle manufacturer.
    pub v_mfgr: VehicleManufacturerName,
    /// Vehicle brand.
    pub v_brand: VehicleBrandName,
    /// Vehicle type
    pub v_type: &'a str,
    /// Variable length comment
    pub v_license: &'a str,
}

impl Display for Vehicle<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|",
            self.v_vehiclekey, self.v_mfgr, self.v_brand, self.v_type, self.v_license
        )
    }
}

/// Generator for Vehicle table data
#[derive(Debug, Clone)]
pub struct VehicleGenerator<'a> {
    scale_factor: f64,
    part: i32,
    part_count: i32,
    distributions: &'a Distributions,
    text_pool: &'a TextPool,
}

impl<'a> VehicleGenerator<'a> {
    /// Base scale for vehicle generation
    const SCALE_BASE: i32 = 100;

    // Constants for vehicle generation
    const NAME_WORDS: i32 = 5;
    const MANUFACTURER_MIN: i32 = 1;
    const MANUFACTURER_MAX: i32 = 5;
    const BRAND_MIN: i32 = 1;
    const BRAND_MAX: i32 = 5;
    const SIZE_MIN: i32 = 1;
    const SIZE_MAX: i32 = 50;
    const COMMENT_AVERAGE_LENGTH: i32 = 14;

    /// Creates a new VehicleGenerator with the given scale factor
    ///
    /// Note the generator's lifetime is `&'static`. See [`VehicleGenerator`] for
    /// more details.
    pub fn new(scale_factor: f64, part: i32, part_count: i32) -> VehicleGenerator<'static> {
        // Note: use explicit lifetime to ensure this remains `&'static`
        Self::new_with_distributions_and_text_pool(
            scale_factor,
            part,
            part_count,
            Distributions::static_default(),
            TextPool::get_or_init_default(),
        )
    }

    /// Creates a VehicleGenerator with specified distributions and text pool
    pub fn new_with_distributions_and_text_pool<'b>(
        scale_factor: f64,
        part: i32,
        part_count: i32,
        distributions: &'b Distributions,
        text_pool: &'b TextPool,
    ) -> VehicleGenerator<'b> {
        VehicleGenerator {
            scale_factor,
            part,
            part_count,
            distributions,
            text_pool,
        }
    }

    /// Return the row count for the given scale factor and generator part count
    pub fn calculate_row_count(scale_factor: f64, part: i32, part_count: i32) -> i64 {
        GenerateUtils::calculate_row_count(Self::SCALE_BASE, scale_factor, part, part_count)
    }

    /// Returns an iterator over the part rows
    pub fn iter(&self) -> VehicleGeneratorIterator<'a> {
        VehicleGeneratorIterator::new(
            self.distributions,
            self.text_pool,
            GenerateUtils::calculate_start_index(
                Self::SCALE_BASE,
                self.scale_factor,
                self.part,
                self.part_count,
            ),
            Self::calculate_row_count(self.scale_factor, self.part, self.part_count),
        )
    }
}

impl<'a> IntoIterator for VehicleGenerator<'a> {
    type Item = Vehicle<'a>;
    type IntoIter = VehicleGeneratorIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator that generates Vehicle rows
#[derive(Debug)]
pub struct VehicleGeneratorIterator<'a> {
    name_random: RandomStringSequence<'a>,
    manufacturer_random: RandomBoundedInt,
    brand_random: RandomBoundedInt,
    type_random: RandomString<'a>,
    size_random: RandomBoundedInt,
    container_random: RandomString<'a>,
    comment_random: RandomText<'a>,

    start_index: i64,
    row_count: i64,
    index: i64,
}

impl<'a> VehicleGeneratorIterator<'a> {
    fn new(
        distributions: &'a Distributions,
        text_pool: &'a TextPool,
        start_index: i64,
        row_count: i64,
    ) -> Self {
        let mut name_random = RandomStringSequence::new(
            709314158,
            VehicleGenerator::NAME_WORDS,
            distributions.part_colors(),
        );
        let mut manufacturer_random = RandomBoundedInt::new(
            1,
            VehicleGenerator::MANUFACTURER_MIN,
            VehicleGenerator::MANUFACTURER_MAX,
        );
        let mut brand_random = RandomBoundedInt::new(
            46831694,
            VehicleGenerator::BRAND_MIN,
            VehicleGenerator::BRAND_MAX,
        );
        let mut type_random = RandomString::new(1841581359, distributions.part_types());
        let mut size_random = RandomBoundedInt::new(
            1193163244,
            VehicleGenerator::SIZE_MIN,
            VehicleGenerator::SIZE_MAX,
        );
        let mut container_random = RandomString::new(727633698, distributions.part_containers());
        let mut comment_random = RandomText::new(
            804159733,
            text_pool,
            VehicleGenerator::COMMENT_AVERAGE_LENGTH as f64,
        );

        // Advance all generators to the starting position
        name_random.advance_rows(start_index);
        manufacturer_random.advance_rows(start_index);
        brand_random.advance_rows(start_index);
        type_random.advance_rows(start_index);
        size_random.advance_rows(start_index);
        container_random.advance_rows(start_index);
        comment_random.advance_rows(start_index);

        VehicleGeneratorIterator {
            name_random,
            manufacturer_random,
            brand_random,
            type_random,
            size_random,
            container_random,
            comment_random,
            start_index,
            row_count,
            index: 0,
        }
    }

    /// Creates a vehicle with the given key
    fn make_vehicle(&mut self, vehicle_key: i64) -> Vehicle<'a> {
        let manufacturer = self.manufacturer_random.next_value();
        let brand = manufacturer * 10 + self.brand_random.next_value();

        Vehicle {
            v_vehiclekey: vehicle_key,
            v_mfgr: VehicleManufacturerName::new(manufacturer),
            v_brand: VehicleBrandName::new(brand),
            v_type: self.type_random.next_value(),
            v_license: self.comment_random.next_value(),
        }
    }

    /// Calculates the price for a vehicle
    pub fn calculate_vehicle_price(vehicle_key: i64) -> i64 {
        let mut price = 90000;

        // limit contribution to $200
        price += (vehicle_key / 10) % 20001;
        price += (vehicle_key % 1000) * 100;

        price
    }
}

impl<'a> Iterator for VehicleGeneratorIterator<'a> {
    type Item = Vehicle<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.row_count {
            return None;
        }

        let vehicle = self.make_vehicle(self.start_index + self.index + 1);

        self.name_random.row_finished();
        self.manufacturer_random.row_finished();
        self.brand_random.row_finished();
        self.type_random.row_finished();
        self.size_random.row_finished();
        self.container_random.row_finished();
        self.comment_random.row_finished();

        self.index += 1;

        Some(vehicle)
    }
}

/// A Driver name, formatted as `"Driver#<n>"`
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DriverName(i64);

impl DriverName {
    /// Creates a new DriverName with the given value
    pub fn new(value: i64) -> Self {
        DriverName(value)
    }
}

impl Display for DriverName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Driver#{:09}", self.0)
    }
}

/// Records for the Driver table.
///
/// The Display trait is implemented to format the line item data as a string
/// in the default TPC-H 'tbl' format.
///
/// ```text
/// 1|Driver#000000001| N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ|17|27-918-335-1736|5755.94|each slyly above the careful|
/// 2|Driver#000000002|89eJ5ksX3ImxJQBvxObC,|5|15-679-861-2259|4032.68| slyly bold instructions. idle dependen|
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Driver {
    /// Primary key
    pub d_driverkey: i64,
    /// Driver name.
    pub d_name: DriverName,
    /// Driver address
    pub d_address: RandomAlphaNumericInstance,
    /// Region name
    pub d_region: String,
    /// Nation name
    pub d_nation: String,
    /// Driver phone number
    pub d_phone: PhoneNumberInstance,
}

impl Display for Driver {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|{}|",
            self.d_driverkey,
            self.d_name,
            self.d_address,
            self.d_region,
            self.d_nation,
            self.d_phone
        )
    }
}

/// Generator for Driver table data
#[derive(Debug, Clone)]
pub struct DriverGenerator<'a> {
    scale_factor: f64,
    part: i32,
    part_count: i32,
    distributions: &'a Distributions,
    text_pool: &'a TextPool,
}

impl<'a> DriverGenerator<'a> {
    /// Base scale for Driver generation
    const SCALE_BASE: i32 = 500;

    /// Base scale for vehicle-driver generation
    const DRIVERS_PER_VEHICLE: i32 = 4;

    // Constants for Driver generation
    const ACCOUNT_BALANCE_MIN: i32 = -99999;
    const ACCOUNT_BALANCE_MAX: i32 = 999999;
    const ADDRESS_AVERAGE_LENGTH: i32 = 25;
    const COMMENT_AVERAGE_LENGTH: i32 = 63;

    // Better Business Bureau comment constants
    pub const BBB_BASE_TEXT: &'static str = "Customer ";
    pub const BBB_COMPLAINT_TEXT: &'static str = "Complaints";
    pub const BBB_RECOMMEND_TEXT: &'static str = "Recommends";
    pub const BBB_COMMENT_LENGTH: usize =
        Self::BBB_BASE_TEXT.len() + Self::BBB_COMPLAINT_TEXT.len();
    pub const BBB_COMMENTS_PER_SCALE_BASE: i32 = 10;
    pub const BBB_COMPLAINT_PERCENT: i32 = 50;

    /// Creates a new DriverGenerator with the given scale factor
    ///
    /// Note the generator's lifetime is `&'static`. See [`DriverGenerator`] for
    /// more details.
    pub fn new(scale_factor: f64, part: i32, part_count: i32) -> DriverGenerator<'static> {
        // Note: use explicit lifetime to ensure this remains `&'static`
        Self::new_with_distributions_and_text_pool(
            scale_factor,
            part,
            part_count,
            Distributions::static_default(),
            TextPool::get_or_init_default(),
        )
    }

    /// Creates a DriverGenerator with specified distributions and text pool
    pub fn new_with_distributions_and_text_pool<'b>(
        scale_factor: f64,
        part: i32,
        part_count: i32,
        distributions: &'b Distributions,
        text_pool: &'b TextPool,
    ) -> DriverGenerator<'b> {
        DriverGenerator {
            scale_factor,
            part,
            part_count,
            distributions,
            text_pool,
        }
    }

    /// Return the row count for the given scale factor and generator part count
    pub fn calculate_row_count(scale_factor: f64, part: i32, part_count: i32) -> i64 {
        GenerateUtils::calculate_row_count(Self::SCALE_BASE, scale_factor, part, part_count)
    }

    /// Returns an iterator over the Driver rows
    pub fn iter(&self) -> DriverGeneratorIterator<'a> {
        DriverGeneratorIterator::new(
            self.distributions,
            self.text_pool,
            GenerateUtils::calculate_start_index(
                Self::SCALE_BASE,
                self.scale_factor,
                self.part,
                self.part_count,
            ),
            Self::calculate_row_count(self.scale_factor, self.part, self.part_count),
        )
    }
}

impl<'a> IntoIterator for DriverGenerator<'a> {
    type Item = Driver;
    type IntoIter = DriverGeneratorIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator that generates Driver rows
#[derive(Debug)]
pub struct DriverGeneratorIterator<'a> {
    address_random: RandomAlphaNumeric,
    nation_key_random: RandomBoundedInt,
    phone_random: RandomPhoneNumber,
    account_balance_random: RandomBoundedInt,
    comment_random: RandomText<'a>,
    bbb_comment_random: RandomBoundedInt,
    bbb_junk_random: RowRandomInt,
    bbb_offset_random: RowRandomInt,
    bbb_type_random: RandomBoundedInt,

    // Add references to distributions
    nations: &'a Distribution,
    regions: &'a Distribution,

    start_index: i64,
    row_count: i64,
    index: i64,
}

impl<'a> DriverGeneratorIterator<'a> {
    fn new(
        distributions: &'a Distributions,
        text_pool: &'a TextPool,
        start_index: i64,
        row_count: i64,
    ) -> Self {
        let mut address_random =
            RandomAlphaNumeric::new(706178559, DriverGenerator::ADDRESS_AVERAGE_LENGTH);
        let mut nation_key_random =
            RandomBoundedInt::new(110356601, 0, (distributions.nations().size() - 1) as i32);
        let mut phone_random = RandomPhoneNumber::new(884434366);
        let mut account_balance_random = RandomBoundedInt::new(
            962338209,
            DriverGenerator::ACCOUNT_BALANCE_MIN,
            DriverGenerator::ACCOUNT_BALANCE_MAX,
        );
        let mut comment_random = RandomText::new(
            1341315363,
            text_pool,
            DriverGenerator::COMMENT_AVERAGE_LENGTH as f64,
        );
        let mut bbb_comment_random =
            RandomBoundedInt::new(202794285, 1, DriverGenerator::SCALE_BASE);
        let mut bbb_junk_random = RowRandomInt::new(263032577, 1);
        let mut bbb_offset_random = RowRandomInt::new(715851524, 1);
        let mut bbb_type_random = RandomBoundedInt::new(753643799, 0, 100);

        // Advance all generators to the starting position
        address_random.advance_rows(start_index);
        nation_key_random.advance_rows(start_index);
        phone_random.advance_rows(start_index);
        account_balance_random.advance_rows(start_index);
        comment_random.advance_rows(start_index);
        bbb_comment_random.advance_rows(start_index);
        bbb_junk_random.advance_rows(start_index);
        bbb_offset_random.advance_rows(start_index);
        bbb_type_random.advance_rows(start_index);

        DriverGeneratorIterator {
            address_random,
            nation_key_random,
            phone_random,
            account_balance_random,
            comment_random,
            bbb_comment_random,
            bbb_junk_random,
            bbb_offset_random,
            bbb_type_random,

            // Initialize the new fields
            nations: distributions.nations(),
            regions: distributions.regions(),

            start_index,
            row_count,
            index: 0,
        }
    }

    /// Creates a Driver with the given key
    fn make_driver(&mut self, driver_key: i64) -> Driver {
        let nation_key = self.nation_key_random.next_value();
        let nation = self.nations.get_value(nation_key as usize);
        let region = self
            .regions
            .get_value(self.nations.get_weight(nation_key as usize) as usize);

        Driver {
            d_driverkey: driver_key,
            d_name: DriverName::new(driver_key),
            d_address: self.address_random.next_value(),
            d_region: region.to_string(), // Convert &str to String
            d_nation: nation.to_string(), // Convert &str to String
            d_phone: self.phone_random.next_value(nation_key as i64),
        }
    }

    /// Selects a driver for a vehicle, with drivers table 5x the size of vehicles table
    pub fn select_driver(vehicle_key: i64, driver_number: i64, scale_factor: f64) -> i64 {
        // Use supplier generator's scale base
        let mut driver_count = (VehicleGenerator::SCALE_BASE as f64 * scale_factor) as i64;
        driver_count = driver_count.max(1);
        ((vehicle_key
            + (driver_number
                * ((driver_count / DriverGenerator::DRIVERS_PER_VEHICLE as i64)
                    + ((vehicle_key - 1) / driver_count))))
            % driver_count)
            + 1
    }
}

impl Iterator for DriverGeneratorIterator<'_> {
    type Item = Driver;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.row_count {
            return None;
        }

        let driver = self.make_driver(self.start_index + self.index + 1);

        self.address_random.row_finished();
        self.nation_key_random.row_finished();
        self.phone_random.row_finished();
        self.account_balance_random.row_finished();
        self.comment_random.row_finished();
        self.bbb_comment_random.row_finished();
        self.bbb_junk_random.row_finished();
        self.bbb_offset_random.row_finished();
        self.bbb_type_random.row_finished();

        self.index += 1;

        Some(driver)
    }
}

/// A Customer Name, formatted as `"Customer#<n>"`
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CustomerName(i64);

impl CustomerName {
    /// Creates a new CustomerName with the given value
    pub fn new(value: i64) -> Self {
        CustomerName(value)
    }
}

impl Display for CustomerName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Customer#{:09}", self.0)
    }
}

/// The CUSTOMER table
///
/// The Display trait is implemented to format the line item data as a string
/// in the default TPC-H 'tbl' format.
///
/// ```text
/// 1|Customer#000000001|IVhzIApeRb ot,c,E|15|25-989-741-2988|711.56|BUILDING|to the even, regular platelets. regular, ironic epitaphs nag e|
/// 2|Customer#000000002|XSTf4,NCwDVaWNe6tEgvwfmRchLXak|13|23-768-687-3665|121.65|AUTOMOBILE|l accounts. blithely ironic theodolites integrate boldly: caref|
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Customer<'a> {
    /// Primary key
    pub c_custkey: i64,
    /// Customer name
    pub c_name: CustomerName,
    /// Customer address
    pub c_address: RandomAlphaNumericInstance,
    /// Region name
    pub c_region: &'a str,
    /// Nation name
    pub c_nation: &'a str,
    /// Customer phone number
    pub c_phone: PhoneNumberInstance,
}

impl Display for Customer<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|{}|",
            self.c_custkey, self.c_name, self.c_address, self.c_region, self.c_nation, self.c_phone,
        )
    }
}

/// Generator for Customer table data
#[derive(Debug, Clone)]
pub struct CustomerGenerator<'a> {
    scale_factor: f64,
    part: i32,
    part_count: i32,
    distributions: &'a Distributions,
    text_pool: &'a TextPool,
}

impl<'a> CustomerGenerator<'a> {
    /// Base scale for customer generation
    const SCALE_BASE: i32 = 30_000;

    // Constants for customer generation
    const ACCOUNT_BALANCE_MIN: i32 = -99999;
    const ACCOUNT_BALANCE_MAX: i32 = 999999;
    const ADDRESS_AVERAGE_LENGTH: i32 = 25;
    const COMMENT_AVERAGE_LENGTH: i32 = 73;

    /// Creates a new CustomerGenerator with the given scale factor
    ///
    /// Note the generator's lifetime is `&'static`. See [`CustomerGenerator`] for
    /// more details.
    pub fn new(scale_factor: f64, part: i32, part_count: i32) -> CustomerGenerator<'static> {
        // Note: use explicit lifetime to ensure this remains `&'static`
        Self::new_with_distributions_and_text_pool(
            scale_factor,
            part,
            part_count,
            Distributions::static_default(),
            TextPool::get_or_init_default(),
        )
    }

    /// Creates a CustomerGenerator with specified distributions and text pool
    pub fn new_with_distributions_and_text_pool<'b>(
        scale_factor: f64,
        part: i32,
        part_count: i32,
        distributions: &'b Distributions,
        text_pool: &'b TextPool,
    ) -> CustomerGenerator<'b> {
        CustomerGenerator {
            scale_factor,
            part,
            part_count,
            distributions,
            text_pool,
        }
    }

    /// Return the row count for the given scale factor and generator part count
    pub fn calculate_row_count(scale_factor: f64, part: i32, part_count: i32) -> i64 {
        GenerateUtils::calculate_row_count(Self::SCALE_BASE, scale_factor, part, part_count)
    }

    /// Returns an iterator over the customer rows
    pub fn iter(&self) -> CustomerGeneratorIterator<'a> {
        CustomerGeneratorIterator::new(
            self.distributions,
            self.text_pool,
            GenerateUtils::calculate_start_index(
                Self::SCALE_BASE,
                self.scale_factor,
                self.part,
                self.part_count,
            ),
            Self::calculate_row_count(self.scale_factor, self.part, self.part_count),
        )
    }
}

impl<'a> IntoIterator for CustomerGenerator<'a> {
    type Item = Customer<'a>;
    type IntoIter = CustomerGeneratorIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator that generates Customer rows
#[derive(Debug)]
pub struct CustomerGeneratorIterator<'a> {
    address_random: RandomAlphaNumeric,
    nation_key_random: RandomBoundedInt,
    phone_random: RandomPhoneNumber,

    start_index: i64,
    row_count: i64,
    index: i64,
    nations: &'a Distribution,
    regions: &'a Distribution,
}

impl<'a> CustomerGeneratorIterator<'a> {
    fn new(
        distributions: &'a Distributions,
        text_pool: &'a TextPool,
        start_index: i64,
        row_count: i64,
    ) -> Self {
        let mut address_random =
            RandomAlphaNumeric::new(881155353, CustomerGenerator::ADDRESS_AVERAGE_LENGTH);
        let mut nation_key_random =
            RandomBoundedInt::new(1489529863, 0, (distributions.nations().size() - 1) as i32);
        let mut phone_random = RandomPhoneNumber::new(1521138112);
        let mut account_balance_random = RandomBoundedInt::new(
            298370230,
            CustomerGenerator::ACCOUNT_BALANCE_MIN,
            CustomerGenerator::ACCOUNT_BALANCE_MAX,
        );
        let mut market_segment_random =
            RandomString::new(1140279430, distributions.market_segments());
        let mut comment_random = RandomText::new(
            1335826707,
            text_pool,
            CustomerGenerator::COMMENT_AVERAGE_LENGTH as f64,
        );

        // Advance all generators to the starting position
        address_random.advance_rows(start_index);
        nation_key_random.advance_rows(start_index);
        phone_random.advance_rows(start_index);
        account_balance_random.advance_rows(start_index);
        market_segment_random.advance_rows(start_index);
        comment_random.advance_rows(start_index);

        CustomerGeneratorIterator {
            address_random,
            phone_random,
            nation_key_random,
            regions: distributions.regions(),
            nations: distributions.nations(),
            start_index,
            row_count,
            index: 0,
        }
    }

    /// Creates a customer with the given key
    fn make_customer(&mut self, customer_key: i64) -> Customer<'a> {
        let nation_key = self.nation_key_random.next_value() as i64;
        let region_key = self.nations.get_weight(nation_key as usize);
        Customer {
            c_custkey: customer_key,
            c_name: CustomerName::new(customer_key),
            c_address: self.address_random.next_value(),
            c_region: self.regions.get_value(region_key as usize),
            c_nation: self.nations.get_value(nation_key as usize),
            c_phone: self.phone_random.next_value(nation_key),
        }
    }
}

impl<'a> Iterator for CustomerGeneratorIterator<'a> {
    type Item = Customer<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.row_count {
            return None;
        }

        let customer = self.make_customer(self.start_index + self.index + 1);

        self.address_random.row_finished();
        self.nation_key_random.row_finished();
        self.phone_random.row_finished();

        self.index += 1;

        Some(customer)
    }
}

/// The TRIP table (fact table)
///
/// The Display trait is implemented to format the trip data as a string
/// in the default TPC-H 'tbl' format.
///
/// ```text
/// 1|150|342|78|2023-04-12 08:30:15|2023-04-12 09:15:42|25.50|4.50|30.00|12.7|
/// 2|43|129|156|2023-04-12 10:05:22|2023-04-12 10:32:18|18.75|3.25|22.00|8.3|
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Trip {
    /// Primary key
    pub t_tripkey: i64,
    /// Foreign key to CUSTOMER
    pub t_custkey: i64,
    /// Foreign key to DRIVER
    pub t_driverkey: i64,
    /// Foreign key to VEHICLE
    pub t_vehiclekey: i64,
    /// Pickup time
    pub t_pickuptime: TPCHDate,
    /// Dropoff time
    pub t_dropofftime: TPCHDate,
    /// Trip fare amount
    pub t_fare: TPCHDecimal,
    /// Trip tip amount
    pub t_tip: TPCHDecimal,
    /// Total amount
    pub t_totalamount: TPCHDecimal,
    /// Trip distance
    pub t_distance: TPCHDecimal,
    /// Trip pickup coordinates
    pub t_pickuploc: Point,
    /// Trip dropoff coordinates
    pub t_dropoffloc: Point,
}

impl Display for Trip {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{:?}|{:?}|",
            self.t_tripkey,
            self.t_custkey,
            self.t_driverkey,
            self.t_vehiclekey,
            self.t_pickuptime,
            self.t_dropofftime,
            self.t_fare,
            self.t_tip,
            self.t_totalamount,
            self.t_distance,
            self.t_pickuploc,
            self.t_dropoffloc,
        )
    }
}

/// Generator for Trip table data
#[derive(Debug, Clone)]
pub struct TripGenerator {
    scale_factor: f64,
    part: i32,
    part_count: i32,
    distributions: Distributions,
    text_pool: TextPool,
    distance_kde: crate::kde::DistanceKDE,
    spatial_gen: SpiderGenerator,
}

impl TripGenerator {
    /// Base scale for trip generation
    const SCALE_BASE: i32 = 6_000_000;

    // Constants for trip generation
    const CUSTOMER_MORTALITY: i32 = 3; // portion with no orders
    const FARE_MIN_PER_MILE: i32 = 150; // $1.50 per mile
    const FARE_MAX_PER_MILE: i32 = 300; // $3.00 per mile
    const TIP_PERCENT_MIN: i32 = 0; // 0% tip
    const TIP_PERCENT_MAX: i32 = 30; // 30% tip
    const TRIP_DURATION_MAX_PER_MILE: i32 = 3; // max 3 minutes per mile

    /// Creates a new TripGenerator with the given scale factor
    pub fn new(scale_factor: f64, part: i32, part_count: i32) -> TripGenerator {
        Self::new_with_distributions_and_text_pool(
            scale_factor,
            part,
            part_count,
            Distributions::static_default(),
            TextPool::get_or_init_default(),
            crate::kde::default_distance_kde(),
            spider_overrides::trip_or_default(SpiderDefaults::trip_default),
        )
    }

    /// Creates a TripGenerator with specified distributions and text pool
    pub fn new_with_distributions_and_text_pool<'b>(
        scale_factor: f64,
        part: i32,
        part_count: i32,
        distributions: &'b Distributions,
        text_pool: &'b TextPool,
        distance_kde: crate::kde::DistanceKDE,
        spatial_gen: SpiderGenerator,
    ) -> TripGenerator {
        TripGenerator {
            scale_factor,
            part,
            part_count,
            distributions: distributions.clone(),
            text_pool: text_pool.clone(),
            distance_kde,
            spatial_gen,
        }
    }

    /// Return the row count for the given scale factor and generator part count
    pub fn calculate_row_count(scale_factor: f64, part: i32, part_count: i32) -> i64 {
        GenerateUtils::calculate_row_count(Self::SCALE_BASE, scale_factor, part, part_count)
    }

    /// Returns an iterator over the trip rows
    pub fn iter(&self) -> TripGeneratorIterator {
        TripGeneratorIterator::new(
            &self.distributions,
            &self.text_pool,
            self.scale_factor,
            GenerateUtils::calculate_start_index(
                Self::SCALE_BASE,
                self.scale_factor,
                self.part,
                self.part_count,
            ),
            GenerateUtils::calculate_row_count(
                Self::SCALE_BASE,
                self.scale_factor,
                self.part,
                self.part_count,
            ),
            self.distance_kde.clone(), // Add the KDE model
            self.spatial_gen.clone(),
        )
    }
}

impl IntoIterator for TripGenerator {
    type Item = Trip;
    type IntoIter = TripGeneratorIterator;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator that generates Trip rows
#[derive(Debug)]
pub struct TripGeneratorIterator {
    customer_key_random: RandomBoundedLong,
    driver_key_random: RandomBoundedLong,
    vehicle_key_random: RandomBoundedLong,
    pickup_date_random: RandomBoundedInt,
    pickup_time_random: dates::RandomTimeOfDay,
    fare_per_mile_random: RandomBoundedInt,
    tip_percent_random: RandomBoundedInt,
    trip_minutes_per_mile_random: RandomBoundedInt,
    distance_kde: crate::kde::DistanceKDE,
    spatial_gen: SpiderGenerator,

    scale_factor: f64,
    start_index: i64,
    row_count: i64,
    max_customer_key: i64,

    index: i64,
    trip_number: i64,
}

impl TripGeneratorIterator {
    fn new(
        _distributions: &Distributions,
        _text_pool: &TextPool,
        scale_factor: f64,
        start_index: i64,
        row_count: i64,
        distance_kde: crate::kde::DistanceKDE,
        spatial_gen: SpiderGenerator,
    ) -> Self {
        // Create all the randomizers
        let max_customer_key = (CustomerGenerator::SCALE_BASE as f64 * scale_factor) as i64;
        let max_driver_key = (DriverGenerator::SCALE_BASE as f64 * scale_factor) as i64;
        let max_vehicle_key = (VehicleGenerator::SCALE_BASE as f64 * scale_factor) as i64;

        let mut customer_key_random =
            RandomBoundedLong::new(921591341, scale_factor >= 30000.0, 1, max_customer_key);
        let mut driver_key_random =
            RandomBoundedLong::new(572982913, scale_factor >= 30000.0, 1, max_driver_key);
        let mut vehicle_key_random =
            RandomBoundedLong::new(135497281, scale_factor >= 30000.0, 1, max_vehicle_key);

        let mut pickup_date_random = RandomBoundedInt::new(
            831649288,
            dates::MIN_GENERATE_DATE,
            dates::MIN_GENERATE_DATE + dates::TOTAL_DATE_RANGE - 1,
        );
        let mut pickup_time_random = dates::RandomTimeOfDay::new(123456789);

        let mut fare_per_mile_random = RandomBoundedInt::new(
            109837462,
            TripGenerator::FARE_MIN_PER_MILE,
            TripGenerator::FARE_MAX_PER_MILE,
        );

        let mut tip_percent_random = RandomBoundedInt::new(
            483912756,
            TripGenerator::TIP_PERCENT_MIN,
            TripGenerator::TIP_PERCENT_MAX,
        );

        let mut trip_minutes_per_mile_random =
            RandomBoundedInt::new(748219567, 1, TripGenerator::TRIP_DURATION_MAX_PER_MILE);

        // Advance all generators to the starting position
        customer_key_random.advance_rows(start_index);
        driver_key_random.advance_rows(start_index);
        vehicle_key_random.advance_rows(start_index);
        pickup_date_random.advance_rows(start_index);
        pickup_time_random.advance_rows(start_index);
        fare_per_mile_random.advance_rows(start_index);
        tip_percent_random.advance_rows(start_index);
        trip_minutes_per_mile_random.advance_rows(start_index);

        TripGeneratorIterator {
            customer_key_random,
            driver_key_random,
            vehicle_key_random,
            pickup_date_random,
            pickup_time_random,
            fare_per_mile_random,
            tip_percent_random,
            trip_minutes_per_mile_random,
            distance_kde,
            spatial_gen,

            scale_factor,
            start_index,
            row_count,
            max_customer_key,

            index: 0,
            trip_number: 0,
        }
    }

    /// Creates a trip with the given key
    fn make_trip(&mut self, trip_key: i64) -> Trip {
        // generate customer key, taking into account customer mortality rate
        let mut customer_key = self.customer_key_random.next_value();
        let mut delta = 1;
        while customer_key % TripGenerator::CUSTOMER_MORTALITY as i64 == 0 {
            customer_key += delta;
            customer_key = customer_key.min(self.max_customer_key);
            delta *= -1;
        }

        let vehicle_key = self.vehicle_key_random.next_value();
        let driver_key = DriverGeneratorIterator::select_driver(
            vehicle_key,
            self.trip_number,
            self.scale_factor,
        );

        let pickup_date_value = self.pickup_date_random.next_value();
        let pickup_time = self.pickup_time_random.next_value();
        let pickup_date = TPCHDate::new_with_time(pickup_date_value, pickup_time);

        // Get distance from KDE model (in miles with decimal precision)
        let mut distance_value = self.distance_kde.generate(trip_key as u64);
        // Hard code distance precision to 8 decimal places
        distance_value = (distance_value * 100_000_000.0).round() / 100_000_000.0;
        let distance = TPCHDecimal((distance_value * 100.0) as i64);

        // Pickup
        let pickuploc_geom = self.spatial_gen.generate(trip_key as u64);
        let pickuploc: Point = pickuploc_geom
            .try_into()
            .expect("Failed to convert to point");
        let pickup_x = pickuploc.x();
        let pickup_y = pickuploc.y();

        // Angle
        let angle_seed = spider_seed_for_index(trip_key as u64, 1234);
        let mut angle_rng = StdRng::seed_from_u64(angle_seed);
        let angle: f64 = angle_rng.gen::<f64>() * std::f64::consts::TAU;

        // Dropoff via polar projection
        let mut dropoff_x = pickup_x + distance_value * angle.cos();
        let mut dropoff_y = pickup_y + distance_value * angle.sin();

        // Hard code coordinate precision to 8 decimal places - milimeter level precision for WGS 84
        dropoff_x = (dropoff_x * 100_000_000.0).round() / 100_000_000.0;
        dropoff_y = (dropoff_y * 100_000_000.0).round() / 100_000_000.0;

        let dropoffloc = Point::new(dropoff_x, dropoff_y);

        let fare_per_mile = self.fare_per_mile_random.next_value() as f64;
        let fare_value = (distance_value * fare_per_mile) / 100.0;
        let fare = TPCHDecimal((fare_value * 100.0) as i64); // Use 100.0 (float) instead of 100 (int)

        let tip_percent = self.tip_percent_random.next_value() as f64; // Convert to f64
        let tip_value = (fare_value * tip_percent) / 100.0; // Use 100.0 instead of 100
        let tip = TPCHDecimal((tip_value * 100.0) as i64); // Use 100.0 instead of 100

        let total_value = fare_value + tip_value;
        let total = TPCHDecimal((total_value * 100.0) as i64); // Use 100.0 instead of 100

        // Calculate trip duration based on distance
        let seconds_per_degree = 180000;
        let duration_seconds = (distance_value * seconds_per_degree as f64).round() as i32;

        // Get hours and minutes from pickup time
        let (pickup_hour, pickup_minute, pickup_second) = pickup_time;
        let total_seconds = (pickup_hour as i32) * 3600
            + (pickup_minute as i32) * 60
            + (pickup_second as i32)
            + duration_seconds;
        let dropoff_hour = ((total_seconds / 3600) % 24) as u8;
        let dropoff_minute = ((total_seconds % 3600) / 60) as u8;
        let dropoff_second = (total_seconds % 60) as u8;
        let day_delta = total_seconds / (24 * 3600);
        let dropoff_day = pickup_date_value + day_delta;

        // Ensure the dropoff day doesn't exceed the maximum date value
        let bounded_dropoff_day = std::cmp::min(
            dropoff_day,
            dates::MIN_GENERATE_DATE + dates::TOTAL_DATE_RANGE - 1,
        );
        let dropoff_date = TPCHDate::new(
            bounded_dropoff_day,
            dropoff_hour,
            dropoff_minute,
            dropoff_second,
        );

        Trip {
            t_tripkey: trip_key,
            t_custkey: customer_key,
            t_driverkey: driver_key,
            t_vehiclekey: vehicle_key,
            t_pickuptime: pickup_date,
            t_dropofftime: dropoff_date,
            t_fare: fare,
            t_tip: tip,
            t_totalamount: total,
            t_distance: distance,
            t_pickuploc: pickuploc,
            t_dropoffloc: dropoffloc,
        }
    }
}

impl Iterator for TripGeneratorIterator {
    type Item = Trip;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.row_count {
            return None;
        }

        let trip = self.make_trip(self.start_index + self.index + 1);

        // Mark all generators as finished with this row
        self.customer_key_random.row_finished();
        self.driver_key_random.row_finished();
        self.vehicle_key_random.row_finished();
        self.pickup_date_random.row_finished();
        self.pickup_time_random.row_finished();
        self.fare_per_mile_random.row_finished();
        self.tip_percent_random.row_finished();
        self.trip_minutes_per_mile_random.row_finished();

        self.index += 1;

        Some(trip)
    }
}

/// Represents a building in the dataset
#[derive(Debug, Clone, PartialEq)]
pub struct Building<'a> {
    /// Unique identifier for the building
    pub b_buildingkey: i64,
    /// Name of the building
    pub b_name: StringSequenceInstance<'a>,
    /// WKT representation of the building's polygon
    pub b_boundary: geo::Polygon,
}

impl Display for Building<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}|{}|{:?}|",
            self.b_buildingkey, self.b_name, self.b_boundary,
        )
    }
}

/// Generator for [`Building`]s
#[derive(Debug, Clone)]
pub struct BuildingGenerator<'a> {
    scale_factor: f64,
    part: i32,
    part_count: i32,
    distributions: &'a Distributions,
    text_pool: &'a TextPool,
    spatial_gen: SpiderGenerator,
}

impl<'a> BuildingGenerator<'a> {
    /// Base scale for vehicle generation
    const SCALE_BASE: i32 = 20_000;
    const NAME_WORDS: i32 = 1;
    const COMMENT_AVERAGE_LENGTH: i32 = 14;

    /// Creates a new BuildingGenerator with the given scale factor
    ///
    /// Note the generator's lifetime is `&'static`. See [`BuildingGenerator`] for
    /// more details.
    pub fn new(scale_factor: f64, part: i32, part_count: i32) -> BuildingGenerator<'static> {
        Self::new_with_distributions_and_text_pool(
            scale_factor,
            part,
            part_count,
            Distributions::static_default(),
            TextPool::get_or_init_default(),
            spider_overrides::building_or_default(SpiderDefaults::building_default),
        )
    }

    /// Creates a BuildingGenerator with specified distributions and text pool
    pub fn new_with_distributions_and_text_pool<'b>(
        scale_factor: f64,
        part: i32,
        part_count: i32,
        distributions: &'b Distributions,
        text_pool: &'b TextPool,
        spatial_gen: SpiderGenerator,
    ) -> BuildingGenerator<'b> {
        BuildingGenerator {
            scale_factor,
            part,
            part_count,
            distributions,
            text_pool,
            spatial_gen,
        }
    }

    /// Return the row count for the given scale factor and generator part count
    pub fn calculate_row_count(scale_factor: f64, part: i32, part_count: i32) -> i64 {
        GenerateUtils::calculate_logarithmic_row_count(
            Self::SCALE_BASE,
            scale_factor,
            part,
            part_count,
        )
    }

    /// Returns an iterator over the part rows
    pub fn iter(&self) -> BuildingGeneratorIterator<'a> {
        BuildingGeneratorIterator::new(
            self.distributions,
            self.text_pool,
            GenerateUtils::calculate_start_index(
                Self::SCALE_BASE,
                self.scale_factor,
                self.part,
                self.part_count,
            ),
            Self::calculate_row_count(self.scale_factor, self.part, self.part_count),
            self.spatial_gen.clone(),
        )
    }
}

impl<'a> IntoIterator for &'a BuildingGenerator<'a> {
    type Item = Building<'a>;
    type IntoIter = BuildingGeneratorIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator that generates Building rows
#[derive(Debug)]
pub struct BuildingGeneratorIterator<'a> {
    name_random: RandomStringSequence<'a>,
    spatial_gen: SpiderGenerator,

    start_index: i64,
    row_count: i64,
    index: i64,
}

impl<'a> BuildingGeneratorIterator<'a> {
    fn new(
        distributions: &'a Distributions,
        text_pool: &'a TextPool,
        start_index: i64,
        row_count: i64,
        spatial_gen: SpiderGenerator,
    ) -> Self {
        let mut name_random = RandomStringSequence::new(
            709314158,
            BuildingGenerator::NAME_WORDS,
            distributions.part_colors(),
        );
        let mut wkt_random = RandomText::new(
            804159733,
            text_pool,
            BuildingGenerator::COMMENT_AVERAGE_LENGTH as f64,
        );

        // Advance all generators to the starting position
        name_random.advance_rows(start_index);
        wkt_random.advance_rows(start_index);

        BuildingGeneratorIterator {
            name_random,
            start_index,
            row_count,
            spatial_gen,

            index: 0,
        }
    }

    /// Creates a part with the given key
    fn make_building(&mut self, building_key: i64) -> Building<'a> {
        let name = self.name_random.next_value();
        let geom = self.spatial_gen.generate(building_key as u64);
        let polygon: geo::Polygon = geom.try_into().expect("Failed to convert to polygon");

        Building {
            b_buildingkey: building_key,
            b_name: name,
            b_boundary: polygon,
        }
    }
}

impl<'a> Iterator for BuildingGeneratorIterator<'a> {
    type Item = Building<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.row_count {
            return None;
        }

        let building = self.make_building(self.start_index + self.index + 1);

        self.name_random.row_finished();

        self.index += 1;

        Some(building)
    }
}

/// Represents a Zone in the dataset
#[derive(Debug, Clone, PartialEq)]
pub struct Zone {
    /// Primary key
    pub z_zonekey: i64,
    /// GERS ID of the zone
    pub z_gersid: String,
    /// Country of the zone
    pub z_country: String,
    /// Region of the zone
    pub z_region: String,
    /// Name of the zone
    pub z_name: String,
    /// Subtype of the zone
    pub z_subtype: String,
    /// Boundary geometry in WKT format
    pub z_boundary: Geometry,
}

impl Display for Zone {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|{}|{:?}|",
            self.z_zonekey,
            self.z_gersid,
            self.z_country,
            self.z_region,
            self.z_name,
            self.z_subtype,
            self.z_boundary
        )
    }
}

/// Generator for [`Zone`]s that loads from a parquet file in S3
#[derive(Debug, Clone)]
pub struct ZoneGenerator {
    scale_factor: f64,
    part: i32,
    part_count: i32,
}

impl ZoneGenerator {
    /// S3 URL for the zones parquet file
    const OVERTURE_RELEASE_DATE: &'static str = "2025-08-20.1";
    const OVERTURE_S3_BUCKET: &'static str = "overturemaps-us-west-2";
    const OVERTURE_S3_PREFIX: &'static str = "release";

    /// Gets the S3 URL for the zones parquet file
    fn get_zones_parquet_url() -> String {
        format!(
            "s3://{}/{}/{}/theme=divisions/type=division_area/*",
            Self::OVERTURE_S3_BUCKET,
            Self::OVERTURE_S3_PREFIX,
            Self::OVERTURE_RELEASE_DATE
        )
    }

    /// Get zone subtypes based on scale factor
    fn get_zone_subtypes_for_scale_factor(scale_factor: f64) -> Vec<&'static str> {
        let mut subtypes = vec!["microhood", "macrohood"];

        if scale_factor >= 10.0 {
            subtypes.extend_from_slice(&["neighborhood", "county"]);
        }

        if scale_factor >= 100.0 {
            subtypes.extend_from_slice(&["localadmin", "locality", "region", "dependency"]);
        }

        if scale_factor >= 1000.0 {
            subtypes.push("country");
        }

        subtypes
    }

    /// Calculate total zones for a given scale factor based on subtype counts
    fn calculate_total_zones_for_scale_factor(scale_factor: f64) -> i64 {
        let subtypes = Self::get_zone_subtypes_for_scale_factor(scale_factor);
        let mut total = 0i64;

        for subtype in subtypes {
            let count = match subtype {
                "microhood" => 74797,
                "macrohood" => 42619,
                "neighborhood" => 298615,
                "county" => 39680,
                "localadmin" => 19007,
                "locality" => 555834,
                "region" => 4714,
                "dependency" => 105,
                "country" => 378,
                _ => 0,
            };
            total += count;
        }

        // Scale down for testing purposes
        if scale_factor < 1.0 {
            total = (total as f64 * scale_factor).ceil() as i64;
        }

        total
    }

    /// Create a new zone generator with streaming approach
    pub fn new(scale_factor: f64, part: i32, part_count: i32) -> Self {
        let start = Instant::now();
        info!(
            "Creating ZoneGenerator with scale_factor={}, part={}, part_count={}",
            scale_factor, part, part_count
        );
        let elapsed = start.elapsed();
        info!("ZoneGenerator created in {:?}", elapsed);

        Self {
            scale_factor,
            part,
            part_count,
        }
    }

    /// Calculate zones per partition
    fn calculate_zones_per_part(&self) -> i64 {
        let total_zones = Self::calculate_total_zones_for_scale_factor(self.scale_factor);
        (total_zones as f64 / self.part_count as f64).ceil() as i64
    }

    /// Calculate offset for this partition
    fn calculate_offset(&self) -> i64 {
        let zones_per_part = self.calculate_zones_per_part();
        (self.part - 1) as i64 * zones_per_part
    }

    /// Load zones for this specific partition using LIMIT and OFFSET
    fn load_partition_zones(&self) -> Result<Vec<Zone>, Box<dyn std::error::Error>> {
        info!(
            "Loading zones for partition {} of {}",
            self.part, self.part_count
        );
        let start_total = Instant::now();

        // Create a connection to DuckDB
        let t0 = Instant::now();
        let conn = Connection::open_in_memory()?;
        debug!("Opened DuckDB connection in {:?}", t0.elapsed());

        // Install and load required extensions
        let t1 = Instant::now();
        conn.execute("INSTALL httpfs;", [])?;
        conn.execute("LOAD httpfs;", [])?;
        conn.execute("INSTALL spatial;", [])?;
        conn.execute("LOAD spatial;", [])?;
        debug!(
            "Installed and loaded DuckDB extensions in {:?}",
            t1.elapsed()
        );

        // Calculate partition parameters
        let zones_per_part = self.calculate_zones_per_part();
        let offset = self.calculate_offset();
        let zones_url = Self::get_zones_parquet_url();
        let subtypes = Self::get_zone_subtypes_for_scale_factor(self.scale_factor);

        info!(
            "Partition {}: LIMIT {} OFFSET {} from {} with subtypes: {:?}",
            self.part, zones_per_part, offset, zones_url, subtypes
        );

        // Build the subtype filter
        let subtype_filter = if subtypes.is_empty() {
            return Err(format!(
                "No subtypes found for scale factor {} in partition {}. This indicates a logic error.",
                self.scale_factor,
                self.part
            ).into());
        } else {
            format!(
                "subtype IN ({})",
                subtypes
                    .iter()
                    .map(|s| format!("'{}'", s))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };

        let query = format!(
            "SELECT
                id as z_gersid,
                country as z_country,
                COALESCE(region, '') as z_region,
                COALESCE(names.primary, '') as z_name,
                subtype as z_subtype,
                ST_AsWKB(geometry) as z_boundary
             FROM read_parquet('{}', hive_partitioning=1)
             WHERE {}
             LIMIT {} OFFSET {};",
            zones_url, subtype_filter, zones_per_part, offset
        );
        debug!("Generated partition query: {}", query);

        // Prepare + execute query
        let t2 = Instant::now();
        let mut stmt = conn.prepare(&query)?;
        debug!("Prepared statement in {:?}", t2.elapsed());

        let t3 = Instant::now();
        let mut rows = stmt.query([])?;
        debug!("Executed query and got row iterator in {:?}", t3.elapsed());

        // Iterate rows and parse geometries
        let mut zones = Vec::new();
        let mut zone_id = offset + 1;

        let t4 = Instant::now();
        while let Ok(Some(row)) = rows.next() {
            let z_gersid: String = row.get(0)?;
            let z_country: String = row.get(1)?;
            let z_region: String = row.get(2)?;
            let z_name: String = row.get(3)?;
            let z_subtype: String = row.get(4)?;
            let wkb_bytes: Vec<u8> = row.get(5)?;
            let geometry: Geometry = Wkb(&wkb_bytes).to_geo()?;

            zones.push(Zone {
                z_zonekey: zone_id,
                z_gersid,
                z_country,
                z_region,
                z_name,
                z_subtype,
                z_boundary: geometry,
            });

            if zones.len() % 1000 == 0 {
                debug!("Loaded {} zones for partition {}", zones.len(), self.part);
            }
            zone_id += 1;
        }

        info!(
            "Partition {} loaded: {} zones in {:?}",
            self.part,
            zones.len(),
            t4.elapsed()
        );

        info!("Total partition load took {:?}", start_total.elapsed());
        Ok(zones)
    }

    /// Return the row count for the given part
    pub fn calculate_row_count(&self) -> i64 {
        let total_zones = Self::calculate_total_zones_for_scale_factor(self.scale_factor);
        let zones_per_part = self.calculate_zones_per_part();
        let offset = self.calculate_offset();

        // Don't exceed total available zones
        std::cmp::min(zones_per_part, total_zones - offset).max(0)
    }

    /// Returns an iterator over the zone rows
    pub fn iter(&self) -> ZoneGeneratorIterator {
        ZoneGeneratorIterator::new(self.clone())
    }
}

impl IntoIterator for ZoneGenerator {
    type Item = Zone;
    type IntoIter = ZoneGeneratorIterator;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator that generates Zone rows by loading partition data on-demand
#[derive(Debug)]
pub struct ZoneGeneratorIterator {
    zones: Vec<Zone>,
    index: usize,
}

impl ZoneGeneratorIterator {
    fn new(generator: ZoneGenerator) -> Self {
        // Load zones for this partition only
        let zones = generator.load_partition_zones().unwrap_or_else(|e| {
            error!(
                "Failed to load zones for partition {}: {}",
                generator.part, e
            );
            Vec::new()
        });

        ZoneGeneratorIterator { zones, index: 0 }
    }
}

impl Iterator for ZoneGeneratorIterator {
    type Item = Zone;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.zones.len() {
            return None;
        }

        let zone = self.zones[self.index].clone();
        self.index += 1;
        Some(zone)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_vehicle_generation() {
        // Create a generator with a small scale factor
        let generator = VehicleGenerator::new(0.01, 1, 1);
        let vehicles: Vec<_> = generator.iter().collect();

        // Should have 0.01 * 200,000 = 2,000 vehicles
        assert_eq!(vehicles.len(), 1);

        // Check first Driver
        let first = &vehicles[0];
        assert_eq!(first.v_vehiclekey, 1);
        assert_eq!(
            first.to_string(),
            "1|Manufacturer#1|Brand#13|PROMO BURNISHED COPPER|ly. slyly ironi|"
        )
    }

    #[test]
    fn test_driver_generation() {
        // Create a generator with a small scale factor
        let generator = DriverGenerator::new(0.01, 1, 1);
        let drivers: Vec<_> = generator.iter().collect();

        // Should have 0.01 * 10,000 = 100 Drivers
        assert_eq!(drivers.len(), 5);

        // Check first Driver
        let first = &drivers[0];
        assert_eq!(first.d_driverkey, 1);
        assert_eq!(
            first.to_string(),
            "1|Driver#000000001| N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ|AMERICA|PERU|27-918-335-1736|"
        )
    }

    #[test]
    fn test_customer_generation() {
        // Create a generator with a small scale factor
        let generator = CustomerGenerator::new(0.01, 1, 1);
        let customers: Vec<_> = generator.iter().collect();

        // Should have 0.01 * 30,000 = 300 customers
        assert_eq!(customers.len(), 300);

        // Check first customer
        let first = &customers[0];
        assert_eq!(first.c_custkey, 1);
        assert_eq!(first.c_name.to_string(), "Customer#000000001");
        assert!(first.c_address.to_string().len() > 0);
        assert!(!first.c_nation.is_empty());
        assert!(!first.c_region.is_empty());
        assert!(first.c_phone.to_string().len() > 0);

        // Verify the string format matches the expected pattern
        let expected_pattern = format!(
            "{}|{}|{}|{}|{}|{}|",
            first.c_custkey,
            first.c_name,
            first.c_address,
            first.c_region,
            first.c_nation,
            first.c_phone
        );
        assert_eq!(first.to_string(), expected_pattern);
    }

    #[test]
    fn test_trip_generation() {
        // Create a generator with a small scale factor
        let generator = TripGenerator::new(0.01, 1, 1);
        let trips: Vec<_> = generator.iter().collect();

        // Should have 0.01 * 6,000,000 = 60,000 trips
        assert_eq!(trips.len(), 60_000);

        // Check first trip
        let first = &trips[0];
        assert_eq!(first.t_tripkey, 1);
        assert!(first.t_custkey > 0);
        assert!(first.t_driverkey > 0);
        assert!(first.t_vehiclekey > 0);

        // Check that pickup date is before or equal to dropoff date
        assert!(first.t_pickuptime <= first.t_dropofftime);

        // Verify the string format matches the expected pattern
        let expected_pattern = format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{:?}|{:?}|",
            first.t_tripkey,
            first.t_custkey,
            first.t_driverkey,
            first.t_vehiclekey,
            first.t_pickuptime,
            first.t_dropofftime,
            first.t_fare,
            first.t_tip,
            first.t_totalamount,
            first.t_distance,
            first.t_pickuploc,
            first.t_dropoffloc,
        );
        assert_eq!(first.to_string(), expected_pattern);

        // Check first Trip
        let first = &trips[1];
        assert_eq!(first.t_tripkey, 2);
        assert_eq!(first.to_string(), "2|172|1|1|1997-12-24 08:47:14|1997-12-24 09:28:57|0.03|0.00|0.04|0.01|POINT(-168.046875 -21.09375)|POINT(-168.03314019 -21.09159343)|");
    }

    #[test]
    fn test_building_generation() {
        // Create a generator with a small scale factor
        let generator = BuildingGenerator::new(0.51, 1, 1);
        let buildings: Vec<_> = generator.iter().collect();

        // Should have 20000 * (1 + log2(0.51)) = 571 buildings
        assert_eq!(buildings.len(), 571);

        // Check first building
        let first = &buildings[0];
        assert_eq!(first.b_buildingkey, 1);

        // Verify the string format matches the expected pattern
        let expected_pattern = format!(
            "{}|{}|{:?}|",
            first.b_buildingkey, first.b_name, first.b_boundary,
        );
        assert_eq!(first.to_string(), expected_pattern);

        // Check first Building
        let first = &buildings[1];
        assert_eq!(first.b_buildingkey, 2);
        assert_eq!(first.to_string(), "2|blush|POLYGON((-53.95503773947216 -4.59336925079586,-53.95553716203489 -4.603649450495837,-53.952720010369774 -4.601933644900541,-53.95223340198092 -4.601479576109057,-53.95084475390658 -4.598929409235666,-53.95503773947216 -4.59336925079586))|")
    }

    #[test]
    fn test_zone_generation() {
        // Create a generator with a small scale factor
        let generator = ZoneGenerator::new(0.001, 1, 1);
        let zones: Vec<_> = generator.into_iter().collect();

        assert_eq!(zones.len(), 118);

        // Check first Driver
        let first = &zones[0];
        assert_eq!(first.z_zonekey, 1);
        assert_eq!(
            first.to_string(),
            "1|635d3a50-3055-44a6-8968-7e7d65dd3f61|WF|WF-UV|Place Sagato-Soane|microhood|POLYGON((-176.1735809 -13.28369,-176.1737479 -13.283821,-176.1738536 -13.2838989,-176.173536 -13.2842404,-176.1725987 -13.2833717,-176.1725033 -13.2833872,-176.1724121 -13.2833876,-176.1723319 -13.283372,-176.1722686 -13.2833485,-176.1720379 -13.283278,-176.172337 -13.2830551,-176.17235 -13.2830455,-176.1724748 -13.283002,-176.1725888 -13.2829915,-176.1727488 -13.2830245,-176.1728399 -13.2830431,-176.1730841 -13.2832721,-176.1733254 -13.2834764,-176.1735809 -13.28369))|"
        )
    }

    #[test]
    fn test_zone_subtype_filters() {
        // Test scale factor 0-10: should only include microhood and macrohood
        let subtypes_0_10 = ZoneGenerator::get_zone_subtypes_for_scale_factor(5.0);
        assert_eq!(subtypes_0_10, vec!["microhood", "macrohood"]);

        // Test scale factor 10-100: should include microhood, macrohood, neighborhood, county
        let subtypes_10_100 = ZoneGenerator::get_zone_subtypes_for_scale_factor(50.0);
        assert_eq!(
            subtypes_10_100,
            vec!["microhood", "macrohood", "neighborhood", "county"]
        );

        // Test scale factor 100-1000: should include all except country
        let subtypes_100_1000 = ZoneGenerator::get_zone_subtypes_for_scale_factor(500.0);
        assert_eq!(
            subtypes_100_1000,
            vec![
                "microhood",
                "macrohood",
                "neighborhood",
                "county",
                "localadmin",
                "locality",
                "region",
                "dependency"
            ]
        );

        // Test scale factor 1000+: should include all subtypes
        let subtypes_1000_plus = ZoneGenerator::get_zone_subtypes_for_scale_factor(2000.0);
        assert_eq!(
            subtypes_1000_plus,
            vec![
                "microhood",
                "macrohood",
                "neighborhood",
                "county",
                "localadmin",
                "locality",
                "region",
                "dependency",
                "country"
            ]
        );
    }
}
