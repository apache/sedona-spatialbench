//! Generators for each TPC-H Tables
use crate::dates;
use crate::decimal::TPCHDecimal;
use crate::distribution::Distribution;
use crate::distribution::Distributions;
use crate::random::RandomPhoneNumber;
use crate::random::RowRandomInt;
use crate::random::{PhoneNumberInstance, RandomBoundedLong};
use crate::random::{RandomAlphaNumeric, RandomAlphaNumericInstance};
use crate::text::TextPool;
use core::fmt;
use std::fmt::Display;

use crate::dates::{GenerateUtils, TPCHDate};
use crate::random::{RandomBoundedInt, RandomString, RandomStringSequence, RandomText};

/// Generator for Nation table data
#[derive(Debug, Clone)]
pub struct NationGenerator<'a> {
    distributions: &'a Distributions,
    text_pool: &'a TextPool,
}

impl Default for NationGenerator<'_> {
    fn default() -> Self {
        // arguments are ignored
        Self::new(1.0, 1, 1)
    }
}

impl<'a> NationGenerator<'a> {
    /// Creates a new NationGenerator with default distributions and text pool
    ///
    /// Nations does not depend on the scale factor or the vehicle number. The signature of
    /// this method is provided to be consistent with the other generators, but the
    /// parameters are ignored. You can use [`NationGenerator::default`] to create a
    /// default generator.
    ///
    /// The generator's lifetime is `&'static` because it references global
    /// [`Distribution]`s and thus can be shared safely between threads.
    pub fn new(_scale_factor: f64, _vehicle: i32, _vehicle_count: i32) -> NationGenerator<'static> {
        // Note: use explicit lifetime to ensure this remains `&'static`
        Self::new_with_distributions_and_text_pool(
            Distributions::static_default(),
            TextPool::get_or_init_default(),
        )
    }

    /// Creates a NationGenerator with the specified distributions and text pool
    pub fn new_with_distributions_and_text_pool<'b>(
        distributions: &'b Distributions,
        text_pool: &'b TextPool,
    ) -> NationGenerator<'b> {
        NationGenerator {
            distributions,
            text_pool,
        }
    }

    /// Returns an iterator over the nation rows
    pub fn iter(&self) -> NationGeneratorIterator<'a> {
        NationGeneratorIterator::new(self.distributions.nations(), self.text_pool)
    }
}

impl<'a> IntoIterator for NationGenerator<'a> {
    type Item = Nation<'a>;
    type IntoIter = NationGeneratorIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// The NATION table
///
/// The Display trait is implemented to format the line item data as a string
/// in the default TPC-H 'tbl' format.
///
/// ```text
/// 0|ALGERIA|0| haggle. carefully final deposits detect slyly agai|
/// 1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon|
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Nation<'a> {
    /// Primary key (0-24)
    pub n_nationkey: i64,
    /// Nation name
    pub n_name: &'a str,
    /// Foreign key to REGION
    pub n_regionkey: i64,
    /// Variable length comment
    pub n_comment: &'a str,
}

impl fmt::Display for Nation<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|",
            self.n_nationkey, self.n_name, self.n_regionkey, self.n_comment
        )
    }
}

impl<'a> Nation<'a> {
    /// Create a new `nation` record with the specified values.
    pub fn new(n_nationkey: i64, n_name: &'a str, n_regionkey: i64, n_comment: &'a str) -> Self {
        Nation {
            n_nationkey,
            n_name,
            n_regionkey,
            n_comment,
        }
    }
}

/// Iterator that generates Nation rows
#[derive(Debug)]
pub struct NationGeneratorIterator<'a> {
    nations: &'a Distribution,
    comment_random: RandomText<'a>,
    index: usize,
}

impl<'a> NationGeneratorIterator<'a> {
    const COMMENT_AVERAGE_LENGTH: i32 = 72;

    fn new(nations: &'a Distribution, text_pool: &'a TextPool) -> Self {
        NationGeneratorIterator {
            nations,
            comment_random: RandomText::new(
                606179079,
                text_pool,
                Self::COMMENT_AVERAGE_LENGTH as f64,
            ),
            index: 0,
        }
    }
}

impl<'a> Iterator for NationGeneratorIterator<'a> {
    type Item = Nation<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.nations.size() {
            return None;
        }

        let nation = Nation {
            // n_nationkey
            n_nationkey: self.index as i64,
            // n_name
            n_name: self.nations.get_value(self.index),
            // n_regionkey
            n_regionkey: self.nations.get_weight(self.index) as i64,
            // n_comment
            n_comment: self.comment_random.next_value(),
        };

        self.comment_random.row_finished();
        self.index += 1;

        Some(nation)
    }
}

/// The REGION table
///
/// The Display trait is implemented to format the line item data as a string
/// in the default TPC-H 'tbl' format.
///
/// ```text
/// 0|AFRICA|lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to |
/// 1|AMERICA|hs use ironic, even requests. s|
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Region<'a> {
    /// Primary key (0-4)
    pub r_regionkey: i64,
    /// Region name (AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST)
    pub r_name: &'a str,
    /// Variable length comment
    pub r_comment: &'a str,
}

impl fmt::Display for Region<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|",
            self.r_regionkey, self.r_name, self.r_comment
        )
    }
}

impl<'a> Region<'a> {
    /// Creates a new `region` record with the specified values.
    pub fn new(r_regionkey: i64, r_name: &'a str, r_comment: &'a str) -> Self {
        Region {
            r_regionkey,
            r_name,
            r_comment,
        }
    }
}

/// Generator for Region table data
#[derive(Debug, Clone)]
pub struct RegionGenerator<'a> {
    distributions: &'a Distributions,
    text_pool: &'a TextPool,
}

impl Default for RegionGenerator<'_> {
    fn default() -> Self {
        // arguments are ignored
        Self::new(1.0, 1, 1)
    }
}

impl<'a> RegionGenerator<'a> {
    /// Creates a new RegionGenerator with default distributions and text pool
    ///
    /// Regions does not depend on the scale factor or the vehicle number. The signature of
    /// this method is provided to be consistent with the other generators, but the
    /// parameters are ignored. You can use [`RegionGenerator::default`] to create a
    /// default generator.
    ///
    /// Note the generator's lifetime is `&'static`. See [`NationGenerator`] for
    /// more details.
    pub fn new(_scale_factor: f64, _vehicle: i32, _vehicle_count: i32) -> RegionGenerator<'static> {
        // Note: use explicit lifetime to ensure this remains `&'static`
        Self::new_with_distributions_and_text_pool(
            Distributions::static_default(),
            TextPool::get_or_init_default(),
        )
    }

    /// Creates a RegionGenerator with the specified distributions and text pool
    pub fn new_with_distributions_and_text_pool<'b>(
        distributions: &'b Distributions,
        text_pool: &'b TextPool,
    ) -> RegionGenerator<'b> {
        RegionGenerator {
            distributions,
            text_pool,
        }
    }

    /// Returns an iterator over the region rows
    pub fn iter(&self) -> RegionGeneratorIterator<'a> {
        RegionGeneratorIterator::new(self.distributions.regions(), self.text_pool)
    }
}

impl<'a> IntoIterator for &'a RegionGenerator<'a> {
    type Item = Region<'a>;
    type IntoIter = RegionGeneratorIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator that generates Region rows
#[derive(Debug)]
pub struct RegionGeneratorIterator<'a> {
    regions: &'a Distribution,
    comment_random: RandomText<'a>,
    index: usize,
}

impl<'a> RegionGeneratorIterator<'a> {
    const COMMENT_AVERAGE_LENGTH: i32 = 72;

    fn new(regions: &'a Distribution, text_pool: &'a TextPool) -> Self {
        RegionGeneratorIterator {
            regions,
            comment_random: RandomText::new(
                1500869201,
                text_pool,
                Self::COMMENT_AVERAGE_LENGTH as f64,
            ),
            index: 0,
        }
    }
}

impl<'a> Iterator for RegionGeneratorIterator<'a> {
    type Item = Region<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.regions.size() {
            return None;
        }

        let region = Region {
            r_regionkey: self.index as i64,
            r_name: self.regions.get_value(self.index),
            r_comment: self.comment_random.next_value(),
        };

        self.comment_random.row_finished();
        self.index += 1;

        Some(region)
    }
}

/// A Vehicle Manufacturer, formatted as `"Manufacturer#<n>"`
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VehicleManufacturerName(i32);

impl VehicleManufacturerName {
    pub fn new(value: i32) -> Self {
        VehicleManufacturerName(value)
    }
}

impl fmt::Display for VehicleManufacturerName {
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

impl fmt::Display for VehicleBrandName {
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

impl fmt::Display for Vehicle<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|",
            self.v_vehiclekey,
            self.v_mfgr,
            self.v_brand,
            self.v_type,
            self.v_license
        )
    }
}

/// Generator for Vehicle table data
#[derive(Debug, Clone)]
pub struct VehicleGenerator<'a> {
    scale_factor: f64,
    vehicle: i32,
    vehicle_count: i32,
    distributions: &'a Distributions,
    text_pool: &'a TextPool,
}

impl<'a> VehicleGenerator<'a> {
    /// Base scale for vehicle generation
    const SCALE_BASE: i32 = 200_000;

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
    /// Note the generator's lifetime is `&'static`. See [`NationGenerator`] for
    /// more details.
    pub fn new(scale_factor: f64, vehicle: i32, vehicle_count: i32) -> VehicleGenerator<'static> {
        // Note: use explicit lifetime to ensure this remains `&'static`
        Self::new_with_distributions_and_text_pool(
            scale_factor,
            vehicle,
            vehicle_count,
            Distributions::static_default(),
            TextPool::get_or_init_default(),
        )
    }

    /// Creates a VehicleGenerator with specified distributions and text pool
    pub fn new_with_distributions_and_text_pool<'b>(
        scale_factor: f64,
        vehicle: i32,
        vehicle_count: i32,
        distributions: &'b Distributions,
        text_pool: &'b TextPool,
    ) -> VehicleGenerator<'b> {
        VehicleGenerator {
            scale_factor,
            vehicle,
            vehicle_count,
            distributions,
            text_pool,
        }
    }

    /// Return the row count for the given scale factor and generator vehicle count
    pub fn calculate_row_count(scale_factor: f64, vehicle: i32, vehicle_count: i32) -> i64 {
        GenerateUtils::calculate_row_count(Self::SCALE_BASE, scale_factor, vehicle, vehicle_count)
    }

    /// Returns an iterator over the vehicle rows
    pub fn iter(&self) -> VehicleGeneratorIterator<'a> {
        VehicleGeneratorIterator::new(
            self.distributions,
            self.text_pool,
            GenerateUtils::calculate_start_index(
                Self::SCALE_BASE,
                self.scale_factor,
                self.vehicle,
                self.vehicle_count,
            ),
            Self::calculate_row_count(self.scale_factor, self.vehicle, self.vehicle_count),
        )
    }
}

impl<'a> IntoIterator for &'a VehicleGenerator<'a> {
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
        let mut brand_random =
            RandomBoundedInt::new(46831694, VehicleGenerator::BRAND_MIN, VehicleGenerator::BRAND_MAX);
        let mut type_random = RandomString::new(1841581359, distributions.part_types());
        let mut size_random =
            RandomBoundedInt::new(1193163244, VehicleGenerator::SIZE_MIN, VehicleGenerator::SIZE_MAX);
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

impl fmt::Display for DriverName {
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

impl fmt::Display for Driver {
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
    vehicle: i32,
    vehicle_count: i32,
    distributions: &'a Distributions,
    text_pool: &'a TextPool,
}

impl<'a> DriverGenerator<'a> {
    /// Base scale for Driver generation
    const SCALE_BASE: i32 = 10_000;

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
    /// Note the generator's lifetime is `&'static`. See [`NationGenerator`] for
    /// more details.
    pub fn new(scale_factor: f64, vehicle: i32, vehicle_count: i32) -> DriverGenerator<'static> {
        // Note: use explicit lifetime to ensure this remains `&'static`
        Self::new_with_distributions_and_text_pool(
            scale_factor,
            vehicle,
            vehicle_count,
            Distributions::static_default(),
            TextPool::get_or_init_default(),
        )
    }

    /// Creates a DriverGenerator with specified distributions and text pool
    pub fn new_with_distributions_and_text_pool<'b>(
        scale_factor: f64,
        vehicle: i32,
        vehicle_count: i32,
        distributions: &'b Distributions,
        text_pool: &'b TextPool,
    ) -> DriverGenerator<'b> {
        DriverGenerator {
            scale_factor,
            vehicle,
            vehicle_count,
            distributions,
            text_pool,
        }
    }

    /// Return the row count for the given scale factor and generator vehicle count
    pub fn calculate_row_count(scale_factor: f64, vehicle: i32, vehicle_count: i32) -> i64 {
        GenerateUtils::calculate_row_count(Self::SCALE_BASE, scale_factor, vehicle, vehicle_count)
    }

    /// Returns an iterator over the Driver rows
    pub fn iter(&self) -> DriverGeneratorIterator<'a> {
        DriverGeneratorIterator::new(
            self.distributions,
            self.text_pool,
            GenerateUtils::calculate_start_index(
                Self::SCALE_BASE,
                self.scale_factor,
                self.vehicle,
                self.vehicle_count,
            ),
            Self::calculate_row_count(self.scale_factor, self.vehicle, self.vehicle_count),
        )
    }
}

impl<'a> IntoIterator for &'a DriverGenerator<'a> {
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
        let region = self.regions.get_value(self.nations.get_weight(nation_key as usize) as usize);

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
        let driver_count = (VehicleGenerator::SCALE_BASE as f64 * scale_factor) as i64;

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

impl fmt::Display for CustomerName {
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

impl fmt::Display for Customer<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|{}|",
            self.c_custkey,
            self.c_name,
            self.c_address,
            self.c_region,
            self.c_nation,
            self.c_phone,
        )
    }
}

/// Generator for Customer table data
#[derive(Debug, Clone)]
pub struct CustomerGenerator<'a> {
    scale_factor: f64,
    vehicle: i32,
    vehicle_count: i32,
    distributions: &'a Distributions,
    text_pool: &'a TextPool,
}

impl<'a> CustomerGenerator<'a> {
    /// Base scale for customer generation
    const SCALE_BASE: i32 = 150_000;

    // Constants for customer generation
    const ACCOUNT_BALANCE_MIN: i32 = -99999;
    const ACCOUNT_BALANCE_MAX: i32 = 999999;
    const ADDRESS_AVERAGE_LENGTH: i32 = 25;
    const COMMENT_AVERAGE_LENGTH: i32 = 73;

    /// Creates a new CustomerGenerator with the given scale factor
    ///
    /// Note the generator's lifetime is `&'static`. See [`NationGenerator`] for
    /// more details.
    pub fn new(scale_factor: f64, vehicle: i32, vehicle_count: i32) -> CustomerGenerator<'static> {
        // Note: use explicit lifetime to ensure this remains `&'static`
        Self::new_with_distributions_and_text_pool(
            scale_factor,
            vehicle,
            vehicle_count,
            Distributions::static_default(),
            TextPool::get_or_init_default(),
        )
    }

    /// Creates a CustomerGenerator with specified distributions and text pool
    pub fn new_with_distributions_and_text_pool<'b>(
        scale_factor: f64,
        vehicle: i32,
        vehicle_count: i32,
        distributions: &'b Distributions,
        text_pool: &'b TextPool,
    ) -> CustomerGenerator<'b> {
        CustomerGenerator {
            scale_factor,
            vehicle,
            vehicle_count,
            distributions,
            text_pool,
        }
    }

    /// Return the row count for the given scale factor and generator vehicle count
    pub fn calculate_row_count(scale_factor: f64, vehicle: i32, vehicle_count: i32) -> i64 {
        GenerateUtils::calculate_row_count(Self::SCALE_BASE, scale_factor, vehicle, vehicle_count)
    }

    /// Returns an iterator over the customer rows
    pub fn iter(&self) -> CustomerGeneratorIterator<'a> {
        CustomerGeneratorIterator::new(
            self.distributions,
            self.text_pool,
            GenerateUtils::calculate_start_index(
                Self::SCALE_BASE,
                self.scale_factor,
                self.vehicle,
                self.vehicle_count,
            ),
            Self::calculate_row_count(self.scale_factor, self.vehicle, self.vehicle_count),
        )
    }
}

impl<'a> IntoIterator for &'a CustomerGenerator<'a> {
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

/// A clerk name, formatted as `"Clerk#<n>"`
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClerkName(i32);

impl ClerkName {
    /// Creates a new ClerkName with the given value
    pub fn new(value: i32) -> Self {
        ClerkName(value)
    }
}

impl fmt::Display for ClerkName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Clerk#{:09}", self.0)
    }
}

/// Order status (F=final, O=open, P=pending)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub enum OrderStatus {
    /// Fulfilled - all line items shipped
    Fulfilled,
    /// Open - no line items shipped
    Open,
    /// Partially fulfilled - some line items shipped
    Pending,
}

impl OrderStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderStatus::Fulfilled => "F",
            OrderStatus::Open => "O",
            OrderStatus::Pending => "P",
        }
    }
}

impl Display for OrderStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// The ORDERS table
///
/// The Display trait is implemented to format the line item data as a string
/// in the default TPC-H 'tbl' format.
///
/// ```text
/// 1|37|O|131251.81|1996-01-02|5-LOW|Clerk#000000951|0|nstructions sleep furiously among |
///  2|79|O|40183.29|1996-12-01|1-URGENT|Clerk#000000880|0| foxes. pending accounts at the pending, silent asymptot|
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Order<'a> {
    /// Primary key
    pub o_orderkey: i64,
    /// Foreign key to CUSTOMER
    pub o_custkey: i64,
    /// Order status (F=final, O=open, P=pending)
    pub o_orderstatus: OrderStatus,
    /// Order total price
    pub o_totalprice: TPCHDecimal,
    /// Order date
    pub o_orderdate: TPCHDate,
    /// Order priority
    pub o_orderpriority: &'a str,
    /// Clerk who processed the order.
    pub o_clerk: ClerkName,
    /// Order shipping priority
    pub o_shippriority: i32,
    /// Variable length comment
    pub o_comment: &'a str,
}

impl fmt::Display for Order<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|",
            self.o_orderkey,
            self.o_custkey,
            self.o_orderstatus,
            self.o_totalprice,
            self.o_orderdate,
            self.o_orderpriority,
            self.o_clerk,
            self.o_shippriority,
            self.o_comment
        )
    }
}

/// Generator for Order table data
#[derive(Debug, Clone)]
pub struct OrderGenerator<'a> {
    scale_factor: f64,
    vehicle: i32,
    vehicle_count: i32,
    distributions: &'a Distributions,
    text_pool: &'a TextPool,
}

impl<'a> OrderGenerator<'a> {
    /// Base scale for order generation
    pub const SCALE_BASE: i32 = 1_500_000;

    // Constants for order generation
    const CUSTOMER_MORTALITY: i32 = 3; // portion with no orders
    const ORDER_DATE_MIN: i32 = dates::MIN_GENERATE_DATE;
    const ORDER_DATE_MAX: i32 =
        Self::ORDER_DATE_MIN + (dates::TOTAL_DATE_RANGE - LineItemGenerator::ITEM_SHIP_DAYS - 1);
    const CLERK_SCALE_BASE: i32 = 1000;

    const LINE_COUNT_MIN: i32 = 1;
    pub const LINE_COUNT_MAX: i32 = 7;

    const COMMENT_AVERAGE_LENGTH: i32 = 49;

    const ORDER_KEY_SPARSE_BITS: i32 = 2;
    const ORDER_KEY_SPARSE_KEEP: i32 = 3;
    /// Creates a new OrderGenerator with the given scale factor
    ///
    /// Note the generator's lifetime is `&'static`. See [`NationGenerator`] for
    /// more details.
    pub fn new(scale_factor: f64, vehicle: i32, vehicle_count: i32) -> OrderGenerator<'static> {
        // Note: use explicit lifetime to ensure this remains `&'static`
        Self::new_with_distributions_and_text_pool(
            scale_factor,
            vehicle,
            vehicle_count,
            Distributions::static_default(),
            TextPool::get_or_init_default(),
        )
    }

    /// Creates a OrderGenerator with specified distributions and text pool
    pub fn new_with_distributions_and_text_pool<'b>(
        scale_factor: f64,
        vehicle: i32,
        vehicle_count: i32,
        distributions: &'b Distributions,
        text_pool: &'b TextPool,
    ) -> OrderGenerator<'b> {
        OrderGenerator {
            scale_factor,
            vehicle,
            vehicle_count,
            distributions,
            text_pool,
        }
    }

    /// Return the row count for the given scale factor and generator vehicle count
    pub fn calculate_row_count(scale_factor: f64, vehicle: i32, vehicle_count: i32) -> i64 {
        GenerateUtils::calculate_row_count(Self::SCALE_BASE, scale_factor, vehicle, vehicle_count)
    }

    /// Returns an iterator over the order rows
    pub fn iter(&self) -> OrderGeneratorIterator<'a> {
        OrderGeneratorIterator::new(
            self.distributions,
            self.text_pool,
            self.scale_factor,
            GenerateUtils::calculate_start_index(
                Self::SCALE_BASE,
                self.scale_factor,
                self.vehicle,
                self.vehicle_count,
            ),
            Self::calculate_row_count(self.scale_factor, self.vehicle, self.vehicle_count),
        )
    }

    /// Creates the order date random generator
    pub fn create_order_date_random() -> RandomBoundedInt {
        RandomBoundedInt::new(1066728069, Self::ORDER_DATE_MIN, Self::ORDER_DATE_MAX)
    }

    /// Creates the line count random generator
    pub fn create_line_count_random() -> RandomBoundedInt {
        RandomBoundedInt::new(1434868289, Self::LINE_COUNT_MIN, Self::LINE_COUNT_MAX)
    }

    /// Creates an order key from an index
    pub fn make_order_key(order_index: i64) -> i64 {
        let low_bits = order_index & ((1 << Self::ORDER_KEY_SPARSE_KEEP) - 1);

        let mut ok = order_index;
        ok >>= Self::ORDER_KEY_SPARSE_KEEP;
        ok <<= Self::ORDER_KEY_SPARSE_BITS;
        ok <<= Self::ORDER_KEY_SPARSE_KEEP;
        ok += low_bits;

        ok
    }
}

impl<'a> IntoIterator for &'a OrderGenerator<'a> {
    type Item = Order<'a>;
    type IntoIter = OrderGeneratorIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator that generates Order rows
#[derive(Debug)]
pub struct OrderGeneratorIterator<'a> {
    order_date_random: RandomBoundedInt,
    line_count_random: RandomBoundedInt,
    customer_key_random: RandomBoundedLong,
    order_priority_random: RandomString<'a>,
    clerk_random: RandomBoundedInt,
    comment_random: RandomText<'a>,

    // For line item simulation to determine order status
    line_quantity_random: RandomBoundedInt,
    line_discount_random: RandomBoundedInt,
    line_tax_random: RandomBoundedInt,
    line_vehicle_key_random: RandomBoundedLong,
    line_ship_date_random: RandomBoundedInt,

    start_index: i64,
    row_count: i64,
    max_customer_key: i64,

    index: i64,
}
impl<'a> OrderGeneratorIterator<'a> {
    fn new(
        distributions: &'a Distributions,
        text_pool: &'a TextPool,
        scale_factor: f64,
        start_index: i64,
        row_count: i64,
    ) -> Self {
        let mut order_date_random = OrderGenerator::create_order_date_random();
        let mut line_count_random = OrderGenerator::create_line_count_random();

        let max_customer_key = (CustomerGenerator::SCALE_BASE as f64 * scale_factor) as i64;

        let mut customer_key_random =
            RandomBoundedLong::new(851767375, scale_factor >= 30000.0, 1, max_customer_key);

        let mut order_priority_random =
            RandomString::new(591449447, distributions.order_priority());

        let max_clerk = (scale_factor * OrderGenerator::CLERK_SCALE_BASE as f64)
            .max(OrderGenerator::CLERK_SCALE_BASE as f64) as i32;
        let mut clerk_random = RandomBoundedInt::new(1171034773, 1, max_clerk);

        let mut comment_random = RandomText::new(
            276090261,
            text_pool,
            OrderGenerator::COMMENT_AVERAGE_LENGTH as f64,
        );

        // For line item simulation
        let mut line_quantity_random = LineItemGenerator::create_quantity_random();
        let mut line_discount_random = LineItemGenerator::create_discount_random();
        let mut line_tax_random = LineItemGenerator::create_tax_random();
        let mut line_vehicle_key_random = LineItemGenerator::create_vehicle_key_random(scale_factor);
        let mut line_ship_date_random = LineItemGenerator::create_ship_date_random();

        // Advance all generators to the starting position
        order_date_random.advance_rows(start_index);
        line_count_random.advance_rows(start_index);
        customer_key_random.advance_rows(start_index);
        order_priority_random.advance_rows(start_index);
        clerk_random.advance_rows(start_index);
        comment_random.advance_rows(start_index);

        line_quantity_random.advance_rows(start_index);
        line_discount_random.advance_rows(start_index);
        line_tax_random.advance_rows(start_index);
        line_vehicle_key_random.advance_rows(start_index);
        line_ship_date_random.advance_rows(start_index);

        OrderGeneratorIterator {
            order_date_random,
            line_count_random,
            customer_key_random,
            order_priority_random,
            clerk_random,
            comment_random,
            line_quantity_random,
            line_discount_random,
            line_tax_random,
            line_vehicle_key_random,
            line_ship_date_random,
            start_index,
            row_count,
            max_customer_key,
            index: 0,
        }
    }

    /// Creates an order with the given index
    fn make_order(&mut self, index: i64) -> Order<'a> {
        let order_key = OrderGenerator::make_order_key(index);

        let order_date = self.order_date_random.next_value();

        // generate customer key, taking into account customer mortality rate
        let mut customer_key = self.customer_key_random.next_value();
        let mut delta = 1;
        while customer_key % OrderGenerator::CUSTOMER_MORTALITY as i64 == 0 {
            customer_key += delta;
            customer_key = customer_key.min(self.max_customer_key);
            delta *= -1;
        }

        let mut total_price = 0;
        let mut shipped_count = 0;

        let line_count = self.line_count_random.next_value();
        for _ in 0..line_count {
            let quantity = self.line_quantity_random.next_value();
            let discount = self.line_discount_random.next_value();
            let tax = self.line_tax_random.next_value();

            let vehicle_key = self.line_vehicle_key_random.next_value();

            let vehicle_price = VehicleGeneratorIterator::calculate_vehicle_price(vehicle_key);
            let extended_price = vehicle_price * quantity as i64;
            let discounted_price = extended_price * (100 - discount as i64);
            total_price += ((discounted_price / 100) * (100 + tax as i64)) / 100;

            let ship_date = self.line_ship_date_random.next_value() + order_date;
            if TPCHDate::is_in_past(ship_date) {
                shipped_count += 1;
            }
        }

        let order_status = if shipped_count == line_count {
            OrderStatus::Fulfilled
        } else if shipped_count > 0 {
            OrderStatus::Pending
        } else {
            OrderStatus::Open
        };

        let clerk_id = self.clerk_random.next_value();
        let clerk_name = ClerkName::new(clerk_id);

        Order {
            o_orderkey: order_key,
            o_custkey: customer_key,
            o_orderstatus: order_status,
            o_totalprice: TPCHDecimal(total_price),
            o_orderdate: TPCHDate::new(order_date),
            o_orderpriority: self.order_priority_random.next_value(),
            o_clerk: clerk_name,
            o_shippriority: 0, // Fixed value per TPC-H spec
            o_comment: self.comment_random.next_value(),
        }
    }
}

impl<'a> Iterator for OrderGeneratorIterator<'a> {
    type Item = Order<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.row_count {
            return None;
        }

        let order = self.make_order(self.start_index + self.index + 1);

        self.order_date_random.row_finished();
        self.line_count_random.row_finished();
        self.customer_key_random.row_finished();
        self.order_priority_random.row_finished();
        self.clerk_random.row_finished();
        self.comment_random.row_finished();

        self.line_quantity_random.row_finished();
        self.line_discount_random.row_finished();
        self.line_tax_random.row_finished();
        self.line_vehicle_key_random.row_finished();
        self.line_ship_date_random.row_finished();

        self.index += 1;

        Some(order)
    }
}

/// The LINEITEM table
///
/// The Display trait is implemented to format the line item data as a string
/// in the default TPC-H 'tbl' format.
///
/// Example
/// ```text
/// 1|156|4|1|17|17954.55|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the|
/// 1|68|9|2|36|34850.16|0.09|0.06|N|O|1996-04-12|1996-02-28|1996-04-20|TAKE BACK RETURN|MAIL|ly final dependencies: slyly bold |
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct LineItem<'a> {
    /// Foreign key to ORDERS
    pub l_orderkey: i64,
    /// Foreign key to VEHICLE
    pub l_vehiclekey: i64,
    /// Foreign key to Driver
    pub l_suppkey: i64,
    /// Line item number within order
    pub l_linenumber: i32,
    /// Quantity ordered
    // TODO: Spec has this as decimal.
    pub l_quantity: i64,
    /// Extended price (l_quantity * p_retailprice)
    pub l_extendedprice: TPCHDecimal,
    /// Discount percentage
    pub l_discount: TPCHDecimal,
    /// Tax percentage
    pub l_tax: TPCHDecimal,
    /// Return flag (R=returned, A=accepted, null=pending)
    pub l_returnflag: &'a str,
    /// Line status (O=ordered, F=fulfilled)
    pub l_linestatus: &'static str,
    /// Date shipped
    pub l_shipdate: TPCHDate,
    /// Date committed to ship
    pub l_commitdate: TPCHDate,
    /// Date received
    pub l_receiptdate: TPCHDate,
    /// Shipping instructions
    pub l_shipinstruct: &'a str,
    /// Shipping mode
    pub l_shipmode: &'a str,
    /// Variable length comment
    pub l_comment: &'a str,
}

impl fmt::Display for LineItem<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|",
            self.l_orderkey,
            self.l_vehiclekey,
            self.l_suppkey,
            self.l_linenumber,
            self.l_quantity,
            self.l_extendedprice,
            self.l_discount,
            self.l_tax,
            self.l_returnflag,
            self.l_linestatus,
            self.l_shipdate,
            self.l_commitdate,
            self.l_receiptdate,
            self.l_shipinstruct,
            self.l_shipmode,
            self.l_comment
        )
    }
}

/// Generator for LineItem table data
#[derive(Debug, Clone)]
pub struct LineItemGenerator<'a> {
    scale_factor: f64,
    vehicle: i32,
    vehicle_count: i32,
    distributions: &'a Distributions,
    text_pool: &'a TextPool,
}

impl<'a> LineItemGenerator<'a> {
    // Constants for line item generation
    const QUANTITY_MIN: i32 = 1;
    const QUANTITY_MAX: i32 = 50;
    const TAX_MIN: TPCHDecimal = TPCHDecimal(0); // 0.00
    const TAX_MAX: TPCHDecimal = TPCHDecimal(8); // 0.08
    const DISCOUNT_MIN: TPCHDecimal = TPCHDecimal(0); // 0.00
    const DISCOUNT_MAX: TPCHDecimal = TPCHDecimal(10); // 0.10
    const VEHICLE_KEY_MIN: i32 = 1;
    const SHIP_DATE_MIN: i32 = 1;
    const SHIP_DATE_MAX: i32 = 121;
    const COMMIT_DATE_MIN: i32 = 30;
    const COMMIT_DATE_MAX: i32 = 90;
    const RECEIPT_DATE_MIN: i32 = 1;
    const RECEIPT_DATE_MAX: i32 = 30;

    pub const ITEM_SHIP_DAYS: i32 = Self::SHIP_DATE_MAX + Self::RECEIPT_DATE_MAX;

    const COMMENT_AVERAGE_LENGTH: i32 = 27;

    /// Creates a new LineItemGenerator with the given scale factor
    ///
    /// Note the generator's lifetime is `&'static`. See [`NationGenerator`] for
    /// more details.
    pub fn new(scale_factor: f64, vehicle: i32, vehicle_count: i32) -> LineItemGenerator<'static> {
        Self::new_with_distributions_and_text_pool(
            scale_factor,
            vehicle,
            vehicle_count,
            Distributions::static_default(),
            TextPool::get_or_init_default(),
        )
    }

    /// Creates a LineItemGenerator with specified distributions and text pool
    pub fn new_with_distributions_and_text_pool<'b>(
        scale_factor: f64,
        vehicle: i32,
        vehicle_count: i32,
        distributions: &'b Distributions,
        text_pool: &'b TextPool,
    ) -> LineItemGenerator<'b> {
        LineItemGenerator {
            scale_factor,
            vehicle,
            vehicle_count,
            distributions,
            text_pool,
        }
    }

    /// Returns an iterator over the line item rows
    pub fn iter(&self) -> LineItemGeneratorIterator<'a> {
        LineItemGeneratorIterator::new(
            self.distributions,
            self.text_pool,
            self.scale_factor,
            GenerateUtils::calculate_start_index(
                OrderGenerator::SCALE_BASE,
                self.scale_factor,
                self.vehicle,
                self.vehicle_count,
            ),
            GenerateUtils::calculate_row_count(
                OrderGenerator::SCALE_BASE,
                self.scale_factor,
                self.vehicle,
                self.vehicle_count,
            ),
        )
    }

    /// Creates a quantity random generator
    pub fn create_quantity_random() -> RandomBoundedInt {
        RandomBoundedInt::new_with_seeds_per_row(
            209208115,
            Self::QUANTITY_MIN,
            Self::QUANTITY_MAX,
            OrderGenerator::LINE_COUNT_MAX,
        )
    }

    /// Creates a discount random generator
    pub fn create_discount_random() -> RandomBoundedInt {
        RandomBoundedInt::new_with_seeds_per_row(
            554590007,
            Self::DISCOUNT_MIN.0 as i32,
            Self::DISCOUNT_MAX.0 as i32,
            OrderGenerator::LINE_COUNT_MAX,
        )
    }

    /// Creates a tax random generator
    pub fn create_tax_random() -> RandomBoundedInt {
        RandomBoundedInt::new_with_seeds_per_row(
            721958466,
            Self::TAX_MIN.0 as i32,
            Self::TAX_MAX.0 as i32,
            OrderGenerator::LINE_COUNT_MAX,
        )
    }

    /// Creates a vehicle key random generator
    pub fn create_vehicle_key_random(scale_factor: f64) -> RandomBoundedLong {
        // If scale_factor >= 30000, use long `RandomBoundedLong` otherwise
        // use `RandomBoundedInt` to avoid overflow.
        RandomBoundedLong::new_with_seeds_per_row(
            1808217256,
            scale_factor >= 30000.0,
            Self::VEHICLE_KEY_MIN as i64,
            (VehicleGenerator::SCALE_BASE as f64 * scale_factor) as i64,
            OrderGenerator::LINE_COUNT_MAX,
        )
    }

    /// Creates a ship date random generator
    pub fn create_ship_date_random() -> RandomBoundedInt {
        RandomBoundedInt::new_with_seeds_per_row(
            1769349045,
            Self::SHIP_DATE_MIN,
            Self::SHIP_DATE_MAX,
            OrderGenerator::LINE_COUNT_MAX,
        )
    }
}

impl<'a> IntoIterator for &'a LineItemGenerator<'a> {
    type Item = LineItem<'a>;
    type IntoIter = LineItemGeneratorIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator that generates LineItem rows
#[derive(Debug)]
pub struct LineItemGeneratorIterator<'a> {
    order_date_random: RandomBoundedInt,
    line_count_random: RandomBoundedInt,

    quantity_random: RandomBoundedInt,
    discount_random: RandomBoundedInt,
    tax_random: RandomBoundedInt,

    line_vehicle_key_random: RandomBoundedLong,

    driver_number_random: RandomBoundedInt,

    ship_date_random: RandomBoundedInt,
    commit_date_random: RandomBoundedInt,
    receipt_date_random: RandomBoundedInt,

    returned_flag_random: RandomString<'a>,
    ship_instructions_random: RandomString<'a>,
    ship_mode_random: RandomString<'a>,

    comment_random: RandomText<'a>,

    scale_factor: f64,
    start_index: i64,
    row_count: i64,

    index: i64,
    order_date: i32,
    line_count: i32,
    line_number: i32,
}

impl<'a> LineItemGeneratorIterator<'a> {
    fn new(
        distributions: &'a Distributions,
        text_pool: &'a TextPool,
        scale_factor: f64,
        start_index: i64,
        row_count: i64,
    ) -> Self {
        let mut order_date_random = OrderGenerator::create_order_date_random();
        let mut line_count_random = OrderGenerator::create_line_count_random();

        let mut quantity_random = LineItemGenerator::create_quantity_random();
        let mut discount_random = LineItemGenerator::create_discount_random();
        let mut tax_random = LineItemGenerator::create_tax_random();

        let mut line_vehicle_key_random = LineItemGenerator::create_vehicle_key_random(scale_factor);

        let mut driver_number_random = RandomBoundedInt::new_with_seeds_per_row(
            2095021727,
            0,
            3,
            OrderGenerator::LINE_COUNT_MAX,
        );

        let mut ship_date_random = LineItemGenerator::create_ship_date_random();
        let mut commit_date_random = RandomBoundedInt::new_with_seeds_per_row(
            904914315,
            LineItemGenerator::COMMIT_DATE_MIN,
            LineItemGenerator::COMMIT_DATE_MAX,
            OrderGenerator::LINE_COUNT_MAX,
        );
        let mut receipt_date_random = RandomBoundedInt::new_with_seeds_per_row(
            373135028,
            LineItemGenerator::RECEIPT_DATE_MIN,
            LineItemGenerator::RECEIPT_DATE_MAX,
            OrderGenerator::LINE_COUNT_MAX,
        );

        let mut returned_flag_random = RandomString::new_with_expected_row_count(
            717419739,
            distributions.return_flags(),
            OrderGenerator::LINE_COUNT_MAX,
        );
        let mut ship_instructions_random = RandomString::new_with_expected_row_count(
            1371272478,
            distributions.ship_instructions(),
            OrderGenerator::LINE_COUNT_MAX,
        );
        let mut ship_mode_random = RandomString::new_with_expected_row_count(
            675466456,
            distributions.ship_modes(),
            OrderGenerator::LINE_COUNT_MAX,
        );
        let mut comment_random = RandomText::new_with_expected_row_count(
            1095462486,
            text_pool,
            LineItemGenerator::COMMENT_AVERAGE_LENGTH as f64,
            OrderGenerator::LINE_COUNT_MAX,
        );

        // Advance all generators to the starting position
        order_date_random.advance_rows(start_index);
        line_count_random.advance_rows(start_index);

        quantity_random.advance_rows(start_index);
        discount_random.advance_rows(start_index);
        tax_random.advance_rows(start_index);

        line_vehicle_key_random.advance_rows(start_index);

        driver_number_random.advance_rows(start_index);

        ship_date_random.advance_rows(start_index);
        commit_date_random.advance_rows(start_index);
        receipt_date_random.advance_rows(start_index);

        returned_flag_random.advance_rows(start_index);
        ship_instructions_random.advance_rows(start_index);
        ship_mode_random.advance_rows(start_index);

        comment_random.advance_rows(start_index);

        // generate information for initial order
        let order_date = order_date_random.next_value();
        let line_count = line_count_random.next_value() - 1;

        LineItemGeneratorIterator {
            order_date_random,
            line_count_random,
            quantity_random,
            discount_random,
            tax_random,
            line_vehicle_key_random,
            driver_number_random,
            ship_date_random,
            commit_date_random,
            receipt_date_random,
            returned_flag_random,
            ship_instructions_random,
            ship_mode_random,
            comment_random,
            scale_factor,
            start_index,
            row_count,
            index: 0,
            order_date,
            line_count,
            line_number: 0,
        }
    }

    /// Creates a line item with the given order index
    fn make_line_item(&mut self, order_index: i64) -> LineItem<'a> {
        let order_key = OrderGenerator::make_order_key(order_index);

        let quantity = self.quantity_random.next_value();
        let discount = self.discount_random.next_value();
        let tax = self.tax_random.next_value();

        let vehicle_key = self.line_vehicle_key_random.next_value();

        // let driver_number = self.driver_number_random.next_value() as i64;
        let driver_key = DriverGeneratorIterator::select_driver(
            vehicle_key,
            self.line_number as i64,
            self.scale_factor,
        );

        let vehicle_price = VehicleGeneratorIterator::calculate_vehicle_price(vehicle_key);
        let extended_price = vehicle_price * quantity as i64;

        let mut ship_date = self.ship_date_random.next_value();
        ship_date += self.order_date;
        let mut commit_date = self.commit_date_random.next_value();
        commit_date += self.order_date;
        let mut receipt_date = self.receipt_date_random.next_value();
        receipt_date += ship_date;

        let returned_flag = if TPCHDate::is_in_past(receipt_date) {
            self.returned_flag_random.next_value()
        } else {
            "N"
        };

        let status = if TPCHDate::is_in_past(ship_date) {
            "F" // Fulfilled
        } else {
            "O" // Open
        };

        let ship_instructions = self.ship_instructions_random.next_value();
        let ship_mode = self.ship_mode_random.next_value();
        let comment = self.comment_random.next_value();

        LineItem {
            l_orderkey: order_key,
            l_vehiclekey: vehicle_key,
            l_suppkey: driver_key,
            l_linenumber: (self.line_number + 1),
            l_quantity: quantity as i64,
            l_extendedprice: TPCHDecimal(extended_price),
            l_discount: TPCHDecimal(discount as i64),
            l_tax: TPCHDecimal(tax as i64),
            l_returnflag: returned_flag,
            l_linestatus: status,
            l_shipdate: TPCHDate::new(ship_date),
            l_commitdate: TPCHDate::new(commit_date),
            l_receiptdate: TPCHDate::new(receipt_date),
            l_shipinstruct: ship_instructions,
            l_shipmode: ship_mode,
            l_comment: comment,
        }
    }
}

impl<'a> Iterator for LineItemGeneratorIterator<'a> {
    type Item = LineItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.row_count {
            return None;
        }

        let line_item = self.make_line_item(self.start_index + self.index + 1);
        self.line_number += 1;

        // advance next row only when all lines for the order have been produced
        if self.line_number > self.line_count {
            self.order_date_random.row_finished();
            self.line_count_random.row_finished();

            self.quantity_random.row_finished();
            self.discount_random.row_finished();
            self.tax_random.row_finished();

            self.line_vehicle_key_random.row_finished();
            self.driver_number_random.row_finished();

            self.ship_date_random.row_finished();
            self.commit_date_random.row_finished();
            self.receipt_date_random.row_finished();

            self.returned_flag_random.row_finished();
            self.ship_instructions_random.row_finished();
            self.ship_mode_random.row_finished();

            self.comment_random.row_finished();

            self.index += 1;

            // generate information for next order
            self.line_count = self.line_count_random.next_value() - 1;
            self.order_date = self.order_date_random.next_value();
            self.line_number = 0;
        }

        Some(line_item)
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
}

impl fmt::Display for Trip {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|",
            self.t_tripkey,
            self.t_custkey,
            self.t_driverkey,
            self.t_vehiclekey,
            self.t_pickuptime,
            self.t_dropofftime,
            self.t_fare,
            self.t_tip,
            self.t_totalamount,
            self.t_distance
        )
    }
}

/// Generator for Trip table data
#[derive(Debug, Clone)]
pub struct TripGenerator {
    scale_factor: f64,
    vehicle: i32,
    vehicle_count: i32,
    distributions: Distributions,
    text_pool: TextPool,
    distance_kde: crate::kde::DistanceKDE,
}

impl TripGenerator {
    /// Base scale for trip generation
    const SCALE_BASE: i32 = 1_500_000;

    // Constants for trip generation
    const DISTANCE_MIN: i32 = 1;   // 1.0 miles
    const DISTANCE_MAX: i32 = 500; // 50.0 miles
    const FARE_MIN_PER_MILE: i32 = 150; // $1.50 per mile
    const FARE_MAX_PER_MILE: i32 = 300; // $3.00 per mile
    const TIP_PERCENT_MIN: i32 = 0;     // 0% tip
    const TIP_PERCENT_MAX: i32 = 30;    // 30% tip
    const TRIP_DURATION_MIN_MINUTES: i32 = 5;  // min duration 5 minutes
    const TRIP_DURATION_MAX_PER_MILE: i32 = 3; // max 3 minutes per mile

    /// Creates a new TripGenerator with the given scale factor
    pub fn new(scale_factor: f64, vehicle: i32, vehicle_count: i32) -> TripGenerator {
        Self::new_with_distributions_and_text_pool(
            scale_factor,
            vehicle,
            vehicle_count,
            Distributions::static_default(),
            TextPool::get_or_init_default(),
            crate::kde::default_distance_kde(),
        )
    }

    /// Creates a TripGenerator with specified distributions and text pool
    pub fn new_with_distributions_and_text_pool<'b>(
        scale_factor: f64,
        vehicle: i32,
        vehicle_count: i32,
        distributions: &'b Distributions,
        text_pool: &'b TextPool,
        distance_kde: crate::kde::DistanceKDE,
    ) -> TripGenerator {
        TripGenerator {
            scale_factor,
            vehicle,
            vehicle_count,
            distributions: distributions.clone(),
            text_pool: text_pool.clone(),
            distance_kde,
        }
    }

    /// Return the row count for the given scale factor and generator vehicle count
    pub fn calculate_row_count(scale_factor: f64, vehicle: i32, vehicle_count: i32) -> i64 {
        GenerateUtils::calculate_row_count(Self::SCALE_BASE, scale_factor, vehicle, vehicle_count)
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
                self.vehicle,
                self.vehicle_count,
            ),
            GenerateUtils::calculate_row_count(
                Self::SCALE_BASE,
                self.scale_factor,
                self.vehicle,
                self.vehicle_count,
            ),
            self.distance_kde.clone(), // Add the KDE model
        )
    }
}

impl<'a> IntoIterator for TripGenerator{
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
    distance_random: RandomBoundedInt,
    fare_per_mile_random: RandomBoundedInt,
    tip_percent_random: RandomBoundedInt,
    trip_minutes_per_mile_random: RandomBoundedInt,
    distance_kde: crate::kde::DistanceKDE,

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
    ) -> Self {
        // Create all the randomizers
        let max_customer_key = (CustomerGenerator::SCALE_BASE as f64 * scale_factor) as i64;
        let max_driver_key = (DriverGenerator::SCALE_BASE as f64 * scale_factor) as i64;
        let max_vehicle_key = (VehicleGenerator::SCALE_BASE as f64 * scale_factor) as i64;

        let mut customer_key_random = RandomBoundedLong::new(921591341, scale_factor >= 30000.0, 1, max_customer_key);
        let mut driver_key_random = RandomBoundedLong::new(572982913, scale_factor >= 30000.0, 1, max_driver_key);
        let mut vehicle_key_random = RandomBoundedLong::new(135497281, scale_factor >= 30000.0, 1, max_vehicle_key);

        let mut pickup_date_random = RandomBoundedInt::new(
            831649288,
            dates::MIN_GENERATE_DATE,
            dates::MIN_GENERATE_DATE + dates::TOTAL_DATE_RANGE - TripGenerator::TRIP_DURATION_MAX_PER_MILE * TripGenerator::DISTANCE_MAX / 60 / 24
        );

        let mut distance_random = RandomBoundedInt::new(
            692134278,
            TripGenerator::DISTANCE_MIN,
            TripGenerator::DISTANCE_MAX
        );

        let mut fare_per_mile_random = RandomBoundedInt::new(
            109837462,
            TripGenerator::FARE_MIN_PER_MILE,
            TripGenerator::FARE_MAX_PER_MILE
        );

        let mut tip_percent_random = RandomBoundedInt::new(
            483912756,
            TripGenerator::TIP_PERCENT_MIN,
            TripGenerator::TIP_PERCENT_MAX
        );

        let mut trip_minutes_per_mile_random = RandomBoundedInt::new(
            748219567,
            1,
            TripGenerator::TRIP_DURATION_MAX_PER_MILE
        );

        // Advance all generators to the starting position
        customer_key_random.advance_rows(start_index);
        driver_key_random.advance_rows(start_index);
        vehicle_key_random.advance_rows(start_index);
        pickup_date_random.advance_rows(start_index);
        distance_random.advance_rows(start_index);
        fare_per_mile_random.advance_rows(start_index);
        tip_percent_random.advance_rows(start_index);
        trip_minutes_per_mile_random.advance_rows(start_index);

        TripGeneratorIterator {
            customer_key_random,
            driver_key_random,
            vehicle_key_random,
            pickup_date_random,
            distance_random,
            fare_per_mile_random,
            tip_percent_random,
            trip_minutes_per_mile_random,
            distance_kde, // Store the KDE model

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
        while customer_key % OrderGenerator::CUSTOMER_MORTALITY as i64 == 0 {
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
        let pickup_date = TPCHDate::new(pickup_date_value);

        // let distance_value = self.distance_random.next_value();
        // let distance = TPCHDecimal((distance_value * 10) as i64); // Convert to i64

        // Get distance from KDE model (in miles with decimal precision)
        let distance_value = self.distance_kde.generate();
        // Convert to Decimal with 2 decimal places
        let distance = self.distance_kde.generate_tpch_decimal();

        // Fix multiplication of f64 by integers by using f64 literals
        let fare_per_mile = self.fare_per_mile_random.next_value() as f64;
        let fare_value = (distance_value * fare_per_mile) / 100.0;
        let fare = TPCHDecimal((fare_value * 100.0) as i64); // Use 100.0 (float) instead of 100 (int)

        let tip_percent = self.tip_percent_random.next_value() as f64; // Convert to f64
        let tip_value = (fare_value * tip_percent) / 100.0; // Use 100.0 instead of 100
        let tip = TPCHDecimal((tip_value * 100.0) as i64); // Use 100.0 instead of 100

        let total_value = fare_value + tip_value;
        let total = TPCHDecimal((total_value * 100.0) as i64); // Use 100.0 instead of 100

        // Calculate trip duration in minutes
        let minutes_per_mile = self.trip_minutes_per_mile_random.next_value() as f64;
        let duration_minutes = TripGenerator::TRIP_DURATION_MIN_MINUTES as f64 + (distance_value * minutes_per_mile);
        let dropoff_date_value = pickup_date_value + ((duration_minutes as f64) / (24.0 * 60.0)) as i32;
        let dropoff_date = TPCHDate::new(dropoff_date_value);

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
        }
    }
}

impl<'a> Iterator for TripGeneratorIterator {
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
        self.distance_random.row_finished();
        self.fare_per_mile_random.row_finished();
        self.tip_percent_random.row_finished();
        self.trip_minutes_per_mile_random.row_finished();

        self.index += 1;

        Some(trip)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nation_generator() {
        let generator = NationGenerator::default();
        let nations: Vec<_> = generator.iter().collect();

        // TPC-H typically has 25 nations
        assert_eq!(nations.len(), 25);
    }

    #[test]
    fn test_region_generator() {
        let generator = RegionGenerator::default();
        let regions: Vec<_> = generator.iter().collect();

        // TPC-H typically has 5 regions
        assert_eq!(regions.len(), 5);
    }

    #[test]
    fn test_vehicle_generation() {
        // Create a generator with a small scale factor
        let generator = VehicleGenerator::new(0.01, 1, 1);
        let vehicles: Vec<_> = generator.iter().collect();

        // Should have 0.01 * 200,000 = 2,000 vehicles
        assert_eq!(vehicles.len(), 2000);

        // Check first Driver
        let first = &vehicles[0];
        assert_eq!(first.v_vehiclekey, 1);
        assert_eq!(first.to_string(), "1|Manufacturer#1|Brand#13|PROMO BURNISHED COPPER|ly. slyly ironi|")
    }

    #[test]
    fn test_driver_generation() {
        // Create a generator with a small scale factor
        let generator = DriverGenerator::new(0.01, 1, 1);
        let drivers: Vec<_> = generator.iter().collect();

        // Should have 0.01 * 10,000 = 100 Drivers
        assert_eq!(drivers.len(), 100);

        // Check first Driver
        let first = &drivers[0];
        assert_eq!(first.d_driverkey, 1);
        assert_eq!(first.to_string(), "1|Driver#000000001| N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ|AMERICA|PERU|27-918-335-1736|")
    }

    #[test]
    fn test_customer_generation() {
        // Create a generator with a small scale factor
        let generator = CustomerGenerator::new(0.01, 1, 1);
        let customers: Vec<_> = generator.iter().collect();

        // Should have 0.01 * 150,000 = 1,500 customers
        assert_eq!(customers.len(), 1500);

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
    fn test_order_generation() {
        // Create a generator with a small scale factor
        let generator = OrderGenerator::new(0.01, 1, 1);
        let orders: Vec<_> = generator.iter().collect();

        // Should have 0.01 * 1,500,000 = 15,000 orders
        assert_eq!(orders.len(), 15000);

        // Check first order
        let first = &orders[0];
        assert_eq!(first.o_orderkey, OrderGenerator::make_order_key(1));
        assert!(first.o_custkey > 0);
        assert!(first.o_totalprice > TPCHDecimal::ZERO);

        // Check order status distribution
        let status_counts =
            orders
                .iter()
                .fold(std::collections::HashMap::new(), |mut acc, order| {
                    *acc.entry(&order.o_orderstatus).or_insert(0) += 1;
                    acc
                });

        // Should have multiple order statuses
        assert!(status_counts.len() >= 2);

        // Check customer key distribution - no customer with mortality factor
        assert!(orders
            .iter()
            .all(|o| o.o_custkey % OrderGenerator::CUSTOMER_MORTALITY as i64 != 0));

        // Check order key sparsity
        for (i, order) in orders.iter().enumerate() {
            assert_eq!(
                order.o_orderkey,
                OrderGenerator::make_order_key(i as i64 + 1)
            );
        }
    }

    #[test]
    fn test_trip_generation() {
        // Create a generator with a small scale factor
        let generator = TripGenerator::new(0.01, 1, 1);
        let trips: Vec<_> = generator.iter().collect();

        // Should have 0.01 * 1,000,000 = 10,000 trips
        assert_eq!(trips.len(), 15000);

        // Check first trip
        let first = &trips[0];
        assert_eq!(first.t_tripkey, 1);
        assert!(first.t_custkey > 0);
        assert!(first.t_driverkey > 0);
        assert!(first.t_vehiclekey > 0);

        // Check that pickup date is before or equal to dropoff date
        // TPCHDate doesn't have a .0 field, use date comparison instead
        assert!(first.t_pickuptime <= first.t_dropofftime);

        // Check that the financial values make sense
        assert!(first.t_fare.0 > 0);
        assert!(first.t_tip.0 >= 0); // Tip could be zero
        assert_eq!(first.t_totalamount.0, first.t_fare.0 + first.t_tip.0);
        assert!(first.t_distance.0 > 0);

        // Verify the string format matches the expected pattern
        let expected_pattern = format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|",
            first.t_tripkey,
            first.t_custkey,
            first.t_driverkey,
            first.t_vehiclekey,
            first.t_pickuptime,
            first.t_dropofftime,
            first.t_fare,
            first.t_tip,
            first.t_totalamount,
            first.t_distance
        );
        assert_eq!(first.to_string(), expected_pattern);

        // Check first Trip
        let first = &trips[1];
        assert_eq!(first.t_tripkey, 2);
        assert_eq!(first.to_string(), "2|851|1286|1285|1997-12-24|1997-12-24|37.00|6.00|43.00|1.40|")
    }

    #[test]
    fn test_make_order_key() {
        // Test order key generation logic
        assert_eq!(OrderGenerator::make_order_key(1), 1); // Low values are preserved
        assert_eq!(OrderGenerator::make_order_key(8), 32); // 8 becomes 1000000
        assert_eq!(OrderGenerator::make_order_key(9), 32 + 1); // 9 becomes 1000001
        assert_eq!(OrderGenerator::make_order_key(10), 32 + 2); // 10 becomes 1000010
    }

    #[test]
    fn test_line_item_generation() {
        // Create a generator with a small scale factor
        let generator = LineItemGenerator::new(0.01, 1, 1);
        let line_items: Vec<_> = generator.iter().collect();

        // Check first line item
        let first = &line_items[0];
        assert_eq!(first.l_orderkey, OrderGenerator::make_order_key(1));
        assert_eq!(first.l_linenumber, 1);
        assert!(first.l_vehiclekey > 0);
        assert!(first.l_suppkey > 0);

        assert!(first.l_quantity >= LineItemGenerator::QUANTITY_MIN as i64);
        assert!(first.l_quantity <= LineItemGenerator::QUANTITY_MAX as i64);

        assert!(first.l_discount >= LineItemGenerator::DISCOUNT_MIN);
        assert!(first.l_discount <= LineItemGenerator::DISCOUNT_MAX);

        assert!(first.l_tax >= LineItemGenerator::TAX_MIN);
        assert!(first.l_tax <= LineItemGenerator::TAX_MAX);

        // Verify line numbers are sequential per order
        let mut order_lines = std::collections::HashMap::new();
        for line in &line_items {
            order_lines
                .entry(line.l_orderkey)
                .or_insert_with(Vec::new)
                .push(line.l_linenumber);
        }

        // Check each order's line numbers
        for (_, lines) in order_lines {
            let mut sorted_lines = lines.clone();
            sorted_lines.sort();

            // Line numbers should start at 1 and be sequential
            for (i, line_num) in sorted_lines.iter().enumerate() {
                assert_eq!(*line_num, (i + 1) as i32);
            }
        }

        // Verify return flags and line status distributions
        let return_flags: std::collections::HashSet<_> =
            line_items.iter().map(|l| &l.l_returnflag).collect();

        assert!(return_flags.len() > 1);

        let line_statuses: std::collections::HashSet<_> =
            line_items.iter().map(|l| &l.l_linestatus).collect();

        assert!(!line_statuses.is_empty());
    }

    #[test]
    fn check_iter_static_lifetimes() {
        // Lifetimes of iterators should be independent of the generator that
        // created it. This test case won't compile if that's not the case.

        let _iter: NationGeneratorIterator<'static> = NationGenerator::default().iter();
        let _iter: RegionGeneratorIterator<'static> = RegionGenerator::default().iter();
        let _iter: VehicleGeneratorIterator<'static> = VehicleGenerator::new(0.1, 1, 1).iter();
        let _iter: DriverGeneratorIterator<'static> = DriverGenerator::new(0.1, 1, 1).iter();
        let _iter: CustomerGeneratorIterator<'static> = CustomerGenerator::new(0.1, 1, 1).iter();
        let _iter: OrderGeneratorIterator<'static> = OrderGenerator::new(0.1, 1, 1).iter();
        let _iter: LineItemGeneratorIterator<'static> = LineItemGenerator::new(0.1, 1, 1).iter();
    }
}
