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
use crate::spider_presets::SpiderPresets;
use crate::text::TextPool;
use duckdb::Connection;
use geo::Geometry;
use geo::Point;
use geozero::{wkb::Wkb, ToGeo};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::convert::TryInto;
use std::fmt;
use std::fmt::Display;

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
            SpiderPresets::for_trip_pickups4(),
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
        let distance_value = self.distance_kde.generate(trip_key as u64);
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
        let dropoff_x = pickup_x + distance_value * angle.cos();
        let dropoff_y = pickup_y + distance_value * angle.sin();
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
        // Note: use explicit lifetime to ensure this remains `&'static`
        Self::new_with_distributions_and_text_pool(
            scale_factor,
            part,
            part_count,
            Distributions::static_default(),
            TextPool::get_or_init_default(),
            SpiderPresets::for_building_polygons(),
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
    zones: Vec<Zone>,
    part: i32,
    part_count: i32,
}

impl ZoneGenerator {
    const SCALE_BASE: i32 = 867_102;
    /// S3 URL for the zones parquet file
    const OVERTURE_RELEASE_DATE: &'static str = "2025-06-25.0";
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

    /// Creates a new ZoneGenerator that loads data from S3
    pub fn new(scale_factor: f64, part: i32, part_count: i32) -> ZoneGenerator {
        // construct temporary ZoneGenerator with empty zones
        let mut generator = ZoneGenerator {
            scale_factor,
            part,
            part_count,
            zones: Vec::new(),
        };

        let zones = generator.load_zones_from_s3();
        generator.zones = zones;

        generator
    }

    /// Loads zone data from S3 parquet file using DuckDB
    fn load_zones_from_s3(&self) -> Vec<Zone> {
        // Create a connection to DuckDB
        let conn = Connection::open_in_memory().expect("Failed to open DuckDB connection");

        // Install and load required extensions
        conn.execute("INSTALL httpfs;", [])
            .expect("Failed to install httpfs");
        conn.execute("LOAD httpfs;", [])
            .expect("Failed to load httpfs");
        conn.execute("INSTALL spatial;", [])
            .expect("Failed to install spatial");
        conn.execute("LOAD spatial;", [])
            .expect("Failed to load spatial");

        let zones_url = Self::get_zones_parquet_url();

        // Compute the limit based on scale factor
        let limit = (self.scale_factor * Self::SCALE_BASE as f64).ceil() as i64;

        let query = format!(
            "SELECT
                id as z_gersid,
                country as z_country,
                COALESCE(region, '') as z_region,
                COALESCE(names.primary, '') as z_name,
                subtype as z_subtype,
                ST_AsWKB(geometry) as z_boundary
             FROM read_parquet('{}', hive_partitioning=1)
             WHERE subtype IN ('localadmin', 'locality', 'neighborhood')
             LIMIT {};",
            zones_url, limit
        );

        let mut stmt = conn.prepare(&query).unwrap();
        let mut rows = stmt.query([]).unwrap();

        let mut zones = Vec::new();
        // Counter for primary key
        let mut zone_id = 1;

        while let Ok(Some(row)) = rows.next() {
            let wkb_bytes: Vec<u8> = row.get(5).unwrap();
            let geometry: Geometry = Wkb(&wkb_bytes).to_geo().unwrap();

            zones.push(Zone {
                z_zonekey: zone_id,
                z_gersid: row.get(0).unwrap(),
                z_country: row.get(1).unwrap(),
                z_region: row.get(2).unwrap(),
                z_name: row.get(3).unwrap(),
                z_subtype: row.get(4).unwrap(),
                z_boundary: geometry,
            });
            zone_id += 1;
        }

        zones
    }

    /// Return the row count for the given part
    pub fn calculate_row_count(&self) -> i64 {
        let zone_count = self.zones.len() as i64;

        if self.part_count <= 1 {
            return zone_count;
        }

        // Partition the zones based on part number
        let zones_per_part = (zone_count + self.part_count as i64 - 1) / self.part_count as i64;
        let start = (self.part - 1) as i64 * zones_per_part;
        let end = std::cmp::min(start + zones_per_part, zone_count);

        end - start
    }

    /// Returns an iterator over the zone rows
    pub fn iter(&self) -> ZoneGeneratorIterator {
        let zone_count = self.zones.len() as i64;

        // If there's only one part, return all zones
        if self.part_count <= 1 {
            return ZoneGeneratorIterator {
                zones: self.zones.clone(),
                end_index: zone_count,
                current_index: 0,
            };
        }

        // Otherwise, calculate the correct range for this part
        let zones_per_part = (zone_count + self.part_count as i64 - 1) / self.part_count as i64;
        let start = (self.part - 1) as i64 * zones_per_part;
        let end = std::cmp::min(start + zones_per_part, zone_count);

        ZoneGeneratorIterator {
            zones: self.zones.clone(),
            end_index: end,
            current_index: start,
        }
    }
}

impl IntoIterator for ZoneGenerator {
    type Item = Zone;
    type IntoIter = ZoneGeneratorIterator;

    fn into_iter(self) -> Self::IntoIter {
        let zone_count = self.zones.len() as i64;

        // If there's only one part, return all zones
        if self.part_count <= 1 {
            return ZoneGeneratorIterator {
                zones: self.zones,
                end_index: zone_count,
                current_index: 0,
            };
        }

        // Otherwise, calculate the correct range for this part
        let zones_per_part = (zone_count + self.part_count as i64 - 1) / self.part_count as i64;
        let start = (self.part - 1) as i64 * zones_per_part;
        let end = std::cmp::min(start + zones_per_part, zone_count);

        ZoneGeneratorIterator {
            zones: self.zones,
            end_index: end,
            current_index: start,
        }
    }
}

/// Iterator that provides access to Zone rows
#[derive(Debug)]
pub struct ZoneGeneratorIterator {
    zones: Vec<Zone>,
    end_index: i64,
    current_index: i64,
}

impl Iterator for ZoneGeneratorIterator {
    type Item = Zone;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index >= self.end_index {
            return None;
        }

        let index = self.current_index as usize;
        self.current_index += 1;

        Some(self.zones[index].clone())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = (self.end_index - self.current_index) as usize;
        (remaining, Some(remaining))
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
        assert_eq!(first.to_string(), "2|172|1|1|1997-12-24 08:47:14|1997-12-24 09:28:57|0.03|0.00|0.04|0.01|POINT(-168.046875 -21.09375)|POINT(-168.03314018997426 -21.091593427559978)|");
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
        assert_eq!(first.to_string(), "2|blush|POLYGON((-37.962323825156744 28.065637750265665,-37.94908364554638 28.065637750265665,-37.94908364554638 28.075185613992147,-37.962323825156744 28.075185613992147,-37.962323825156744 28.065637750265665))|")
    }

    #[test]
    fn test_zone_generation() {
        // Create a generator with a small scale factor
        let generator = ZoneGenerator::new(0.001, 1, 1);
        let zones: Vec<_> = generator.into_iter().collect();

        assert_eq!(zones.len(), 868);

        // Check first Driver
        let first = &zones[0];
        assert_eq!(first.z_zonekey, 1);
        assert_eq!(
            first.to_string(),
            "1|b40981d8-1a8b-4b30-bbdc-2a2d941bfa4f|PF||Anapoto|locality|POLYGON((-152.8059003 -22.6387783,-152.8063121 -22.6353325,-152.8063274 -22.6352309,-152.8064935 -22.6352445,-152.806615 -22.6352496,-152.8068727 -22.6352603,-152.8070173 -22.6352663,-152.8072428 -22.6352461,-152.8073888 -22.6352422,-152.8075809 -22.6352564,-152.8076508 -22.6352615,-152.8080525 -22.6353115,-152.8082102 -22.6353388,-152.8083864 -22.6353691,-152.8087408 -22.635439,-152.8089964 -22.6354851,-152.809157 -22.635514,-152.8095701 -22.6355938,-152.8097425 -22.6356095,-152.8099928 -22.6356323,-152.8101359 -22.635648,-152.8103232 -22.6356685,-152.8104963 -22.6356901,-152.8105816 -22.6357006,-152.8108325 -22.6357602,-152.8110029 -22.635792,-152.8113514 -22.6358761,-152.8114181 -22.6358873,-152.8114863 -22.6358986,-152.8116158 -22.63592,-152.8119312 -22.6360031,-152.8122102 -22.6360736,-152.8123122 -22.6360994,-152.8123831 -22.6361173,-152.8125148 -22.6361723,-152.8127572 -22.6362734,-152.8130103 -22.6363624,-152.8131413 -22.6364083,-152.8133375 -22.6364581,-152.8134628 -22.636476,-152.8135086 -22.6364886,-152.8135461 -22.6364991,-152.8136434 -22.6365257,-152.8137188 -22.6365658,-152.8137808 -22.6366066,-152.8138148 -22.6366095,-152.8138596 -22.6366018,-152.8139168 -22.6366298,-152.8139622 -22.6366724,-152.8139927 -22.6366749,-152.8140195 -22.6366771,-152.8140674 -22.6366725,-152.8141116 -22.6366478,-152.8141644 -22.6366325,-152.8141902 -22.6366384,-152.8142412 -22.6366524,-152.8142793 -22.6366823,-152.8142934 -22.6366934,-152.8143238 -22.6367219,-152.8144182 -22.6368091,-152.8144633 -22.6368314,-152.8145004 -22.6368495,-152.8145965 -22.6368568,-152.8146853 -22.6368561,-152.8147697 -22.636891,-152.8148696 -22.6369375,-152.8149052 -22.6369415,-152.8149218 -22.6369433,-152.8150008 -22.6369451,-152.815083 -22.6369908,-152.815123 -22.6370274,-152.8152036 -22.6371007,-152.8153149 -22.6372176,-152.8153466 -22.6372542,-152.8154027 -22.6373164,-152.8154421 -22.6373364,-152.8155528 -22.6373928,-152.8155832 -22.6374082,-152.8158194 -22.6375439,-152.8160403 -22.6376795,-152.8163091 -22.6378993,-152.8164741 -22.6380522,-152.8166172 -22.6381847,-152.8168035 -22.6383597,-152.8169073 -22.6384568,-152.8171645 -22.6387374,-152.8172931 -22.6388667,-152.8175179 -22.6390909,-152.8177225 -22.6392866,-152.8178166 -22.6393713,-152.8178857 -22.6394326,-152.8180028 -22.6395686,-152.8180303 -22.6396,-152.8181478 -22.6397523,-152.8182189 -22.639872,-152.8182598 -22.6399387,-152.8184122 -22.6401837,-152.8185261 -22.6403496,-152.818555 -22.6403916,-152.8186566 -22.64054,-152.8187044 -22.6406086,-152.8188833 -22.6408075,-152.8191328 -22.6411028,-152.8192095 -22.6412048,-152.8192746 -22.6412914,-152.8193023 -22.6413283,-152.819354 -22.6414423,-152.8194053 -22.6415657,-152.8194429 -22.6416539,-152.8194977 -22.6417977,-152.8195891 -22.6419764,-152.819617 -22.6420305,-152.8197665 -22.6423321,-152.8198592 -22.6424946,-152.8199276 -22.6426437,-152.8199735 -22.6427422,-152.8200405 -22.642903,-152.8201318 -22.6430799,-152.8201763 -22.6431404,-152.8202073 -22.6431813,-152.8202858 -22.6432399,-152.8203403 -22.6432676,-152.8204053 -22.6433007,-152.8204607 -22.6433288,-152.8206415 -22.6434824,-152.8207724 -22.6436166,-152.820829 -22.6436746,-152.821004 -22.6439159,-152.8210321 -22.6439545,-152.821152 -22.6441398,-152.8212122 -22.6442326,-152.821266 -22.644324,-152.8213281 -22.6444294,-152.8213708 -22.6445326,-152.8213854 -22.6445668,-152.8214622 -22.6447784,-152.8215387 -22.6448836,-152.8216477 -22.645032,-152.8217318 -22.6451412,-152.8217862 -22.6452316,-152.821811 -22.6453278,-152.8218653 -22.6454378,-152.8220162 -22.6456624,-152.8220364 -22.6457232,-152.8220572 -22.6458025,-152.8220872 -22.6459145,-152.8221389 -22.6460751,-152.8222084 -22.6462216,-152.8222904 -22.6463717,-152.8223335 -22.6464804,-152.8223548 -22.6465769,-152.8223761 -22.6468019,-152.8224082 -22.6469227,-152.8224534 -22.6470922,-152.8224774 -22.647182,-152.8225232 -22.6473511,-152.8225331 -22.6473885,-152.8225542 -22.6474656,-152.822639 -22.6477302,-152.8226439 -22.6477472,-152.8226721 -22.6478443,-152.8226854 -22.6478874,-152.8226995 -22.6479487,-152.8227015 -22.6479808,-152.8226901 -22.6480153,-152.8226823 -22.6480386,-152.8227251 -22.6482412,-152.8227433 -22.6483863,-152.8227508 -22.6484391,-152.8227847 -22.6486052,-152.8228313 -22.6487781,-152.8228548 -22.6489242,-152.8228558 -22.6489888,-152.822865 -22.6493118,-152.8228895 -22.6495633,-152.8229089 -22.6496691,-152.8229373 -22.6498192,-152.8229813 -22.6500556,-152.8230115 -22.6501677,-152.8230307 -22.6502355,-152.823071 -22.6503255,-152.8230744 -22.6503541,-152.8230538 -22.6504032,-152.8230265 -22.6504706,-152.8230115 -22.6505185,-152.8229942 -22.6505741,-152.8229966 -22.6507092,-152.8230028 -22.6507654,-152.8230224 -22.6509441,-152.822995 -22.6512032,-152.8229881 -22.6513635,-152.8229866 -22.6514119,-152.8229882 -22.6516081,-152.8229574 -22.6517221,-152.8229408 -22.6517838,-152.8229368 -22.6518464,-152.8229317 -22.6519622,-152.8229389 -22.6520046,-152.8229607 -22.6521326,-152.8229315 -22.6522542,-152.8229225 -22.6522946,-152.8228486 -22.6524887,-152.8228233 -22.6525773,-152.8227584 -22.6528043,-152.8227214 -22.6531939,-152.8227011 -22.6532685,-152.8226326 -22.6535276,-152.8226274 -22.653541,-152.8226207 -22.6535581,-152.822546 -22.6537508,-152.8224821 -22.6539252,-152.8224712 -22.6539551,-152.8224238 -22.6540843,-152.8222965 -22.6544335,-152.8222917 -22.6544467,-152.8222304 -22.6546037,-152.8221914 -22.654704,-152.8221656 -22.6547803,-152.8221314 -22.6548812,-152.822123 -22.6549062,-152.8220624 -22.6550866,-152.8220384 -22.6551778,-152.8220116 -22.65528,-152.8219518 -22.655403,-152.8219208 -22.6554662,-152.8219003 -22.6555015,-152.8218682 -22.6555557,-152.8218167 -22.6556448,-152.821792 -22.6556805,-152.8217372 -22.65576,-152.8217238 -22.6557794,-152.8216511 -22.6558848,-152.8214509 -22.656136,-152.8213015 -22.6563643,-152.821267 -22.656417,-152.8211502 -22.6565517,-152.8210774 -22.6565791,-152.8210031 -22.6565908,-152.8209492 -22.6565843,-152.8208854 -22.6565765,-152.8208444 -22.6565321,-152.8208213 -22.656448,-152.82081 -22.6564054,-152.8207985 -22.6563878,-152.8207439 -22.6563704,-152.8206667 -22.6563541,-152.8205232 -22.6563533,-152.8204369 -22.6563772,-152.8203162 -22.6564368,-152.8201751 -22.6565374,-152.8199895 -22.6566702,-152.8197213 -22.6568727,-152.8194859 -22.6570406,-152.8192808 -22.6571888,-152.8190232 -22.6574131,-152.8187686 -22.6575772,-152.8185247 -22.6577248,-152.818267 -22.6578656,-152.8182282 -22.657892,-152.8180987 -22.6579807,-152.8177937 -22.6581334,-152.8176976 -22.6579619,-152.8176646 -22.6578903,-152.8175984 -22.6577465,-152.8172706 -22.6570348,-152.8172413 -22.6567272,-152.8172097 -22.6564896,-152.8172255 -22.6563648,-152.8172613 -22.6560833,-152.8171476 -22.6556558,-152.8171164 -22.6555343,-152.8170596 -22.6553129,-152.8170446 -22.6552253,-152.8170072 -22.6549966,-152.8165327 -22.6538796,-152.8161435 -22.6540661,-152.8158883 -22.6538176,-152.8158353 -22.653721,-152.8158 -22.6536229,-152.8157951 -22.6535845,-152.8157551 -22.6535953,-152.8157446 -22.6535605,-152.8155428 -22.6530537,-152.815277 -22.6522363,-152.8151602 -22.6519058,-152.8148256 -22.6520022,-152.8146832 -22.6521046,-152.814827 -22.6511471,-152.8146218 -22.6510436,-152.8139951 -22.6505562,-152.8132902 -22.6501208,-152.8125646 -22.6497519,-152.8123771 -22.6496594,-152.8115954 -22.6493576,-152.8111335 -22.6491795,-152.8110157 -22.6491338,-152.8102632 -22.6490508,-152.8098304 -22.6489638,-152.8093594 -22.6488499,-152.8090844 -22.648791,-152.8090725 -22.6490504,-152.8090328 -22.6491619,-152.8089634 -22.6492704,-152.8088728 -22.6493635,-152.8087937 -22.649426,-152.8087193 -22.6494845,-152.8086213 -22.6495708,-152.8085187 -22.6496436,-152.8084165 -22.6497164,-152.8083148 -22.6497774,-152.8081814 -22.6498444,-152.8080636 -22.6499017,-152.8079406 -22.6499339,-152.8078458 -22.6499682,-152.8077331 -22.650004,-152.8076457 -22.6500306,-152.8075164 -22.6500534,-152.807401 -22.6500629,-152.8074711 -22.6507317,-152.8073518 -22.650987,-152.8072428 -22.6512205,-152.8071899 -22.6513357,-152.8068028 -22.6514097,-152.8060777 -22.6516347,-152.8059191 -22.6507538,-152.8055436 -22.6508052,-152.8046333 -22.650891,-152.8044774 -22.6507755,-152.8046333 -22.6506923,-152.8053217 -22.650326,-152.8051875 -22.6499759,-152.8051888 -22.6498785,-152.8051945 -22.64947,-152.8051649 -22.6489513,-152.8049812 -22.648475,-152.8049481 -22.6479965,-152.8050966 -22.6478349,-152.805622 -22.6472637,-152.8058505 -22.6466584,-152.8062155 -22.6456144,-152.8064066 -22.6452187,-152.8064187 -22.6451712,-152.8067177 -22.6440524,-152.8067904 -22.6434237,-152.8068761 -22.6426821,-152.8068845 -22.6426099,-152.8068886 -22.6425737,-152.8068985 -22.6425316,-152.8067974 -22.6416771,-152.8067746 -22.6414416,-152.8068895 -22.640827,-152.8067316 -22.6405268,-152.8062644 -22.6399417,-152.8057471 -22.6392926,-152.805817 -22.639055,-152.8059003 -22.6387783))|"
        )
    }
}
