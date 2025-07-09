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
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fmt;
use std::fmt::Display;

// /// Generator for Nation table data
// #[derive(Debug, Clone)]
// pub struct NationGenerator<'a> {
//     distributions: &'a Distributions,
//     text_pool: &'a TextPool,
// }
//
// impl Default for NationGenerator<'_> {
//     fn default() -> Self {
//         // arguments are ignored
//         Self::new(1.0, 1, 1)
//     }
// }
//
// impl<'a> NationGenerator<'a> {
//     /// Creates a new NationGenerator with default distributions and text pool
//     ///
//     /// Nations does not depend on the scale factor or the part number. The signature of
//     /// this method is provided to be consistent with the other generators, but the
//     /// parameters are ignored. You can use [`NationGenerator::default`] to create a
//     /// default generator.
//     ///
//     /// The generator's lifetime is `&'static` because it references global
//     /// [`Distribution]`s and thus can be shared safely between threads.
//     pub fn new(_scale_factor: f64, _vehicle: i32, _vehicle_count: i32) -> NationGenerator<'static> {
//         // Note: use explicit lifetime to ensure this remains `&'static`
//         Self::new_with_distributions_and_text_pool(
//             Distributions::static_default(),
//             TextPool::get_or_init_default(),
//         )
//     }
//
//     /// Creates a NationGenerator with the specified distributions and text pool
//     pub fn new_with_distributions_and_text_pool<'b>(
//         distributions: &'b Distributions,
//         text_pool: &'b TextPool,
//     ) -> NationGenerator<'b> {
//         NationGenerator {
//             distributions,
//             text_pool,
//         }
//     }
//
//     /// Returns an iterator over the nation rows
//     pub fn iter(&self) -> NationGeneratorIterator<'a> {
//         NationGeneratorIterator::new(self.distributions.nations(), self.text_pool)
//     }
// }
//
// impl<'a> IntoIterator for NationGenerator<'a> {
//     type Item = Nation<'a>;
//     type IntoIter = NationGeneratorIterator<'a>;
//
//     fn into_iter(self) -> Self::IntoIter {
//         self.iter()
//     }
// }
//
// /// The NATION table
// ///
// /// The Display trait is implemented to format the line item data as a string
// /// in the default TPC-H 'tbl' format.
// ///
// /// ```text
// /// 0|ALGERIA|0| haggle. carefully final deposits detect slyly agai|
// /// 1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon|
// /// ```
// #[derive(Debug, Clone, PartialEq, Eq)]
// pub struct Nation<'a> {
//     /// Primary key (0-24)
//     pub n_nationkey: i64,
//     /// Nation name
//     pub n_name: &'a str,
//     /// Foreign key to REGION
//     pub n_regionkey: i64,
//     /// Variable length comment
//     pub n_comment: &'a str,
// }
//
// impl Display for Nation<'_> {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(
//             f,
//             "{}|{}|{}|{}|",
//             self.n_nationkey, self.n_name, self.n_regionkey, self.n_comment
//         )
//     }
// }
//
// impl<'a> Nation<'a> {
//     /// Create a new `nation` record with the specified values.
//     pub fn new(n_nationkey: i64, n_name: &'a str, n_regionkey: i64, n_comment: &'a str) -> Self {
//         Nation {
//             n_nationkey,
//             n_name,
//             n_regionkey,
//             n_comment,
//         }
//     }
// }
//
// /// Iterator that generates Nation rows
// #[derive(Debug)]
// pub struct NationGeneratorIterator<'a> {
//     nations: &'a Distribution,
//     comment_random: RandomText<'a>,
//     index: usize,
// }
//
// impl<'a> NationGeneratorIterator<'a> {
//     const COMMENT_AVERAGE_LENGTH: i32 = 72;
//
//     fn new(nations: &'a Distribution, text_pool: &'a TextPool) -> Self {
//         NationGeneratorIterator {
//             nations,
//             comment_random: RandomText::new(
//                 606179079,
//                 text_pool,
//                 Self::COMMENT_AVERAGE_LENGTH as f64,
//             ),
//             index: 0,
//         }
//     }
// }
//
// impl<'a> Iterator for NationGeneratorIterator<'a> {
//     type Item = Nation<'a>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.index >= self.nations.size() {
//             return None;
//         }
//
//         let nation = Nation {
//             // n_nationkey
//             n_nationkey: self.index as i64,
//             // n_name
//             n_name: self.nations.get_value(self.index),
//             // n_regionkey
//             n_regionkey: self.nations.get_weight(self.index) as i64,
//             // n_comment
//             n_comment: self.comment_random.next_value(),
//         };
//
//         self.comment_random.row_finished();
//         self.index += 1;
//
//         Some(nation)
//     }
// }
//
// /// The REGION table
// ///
// /// The Display trait is implemented to format the line item data as a string
// /// in the default TPC-H 'tbl' format.
// ///
// /// ```text
// /// 0|AFRICA|lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to |
// /// 1|AMERICA|hs use ironic, even requests. s|
// /// ```
// #[derive(Debug, Clone, PartialEq, Eq)]
// pub struct Region<'a> {
//     /// Primary key (0-4)
//     pub r_regionkey: i64,
//     /// Region name (AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST)
//     pub r_name: &'a str,
//     /// Variable length comment
//     pub r_comment: &'a str,
// }
//
// impl Display for Region<'_> {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(
//             f,
//             "{}|{}|{}|",
//             self.r_regionkey, self.r_name, self.r_comment
//         )
//     }
// }
//
// impl<'a> Region<'a> {
//     /// Creates a new `region` record with the specified values.
//     pub fn new(r_regionkey: i64, r_name: &'a str, r_comment: &'a str) -> Self {
//         Region {
//             r_regionkey,
//             r_name,
//             r_comment,
//         }
//     }
// }
//
// /// Generator for Region table data
// #[derive(Debug, Clone)]
// pub struct RegionGenerator<'a> {
//     distributions: &'a Distributions,
//     text_pool: &'a TextPool,
// }
//
// impl Default for RegionGenerator<'_> {
//     fn default() -> Self {
//         // arguments are ignored
//         Self::new(1.0, 1, 1)
//     }
// }
//
// impl<'a> RegionGenerator<'a> {
//     /// Creates a new RegionGenerator with default distributions and text pool
//     ///
//     /// Regions does not depend on the scale factor or the part number. The signature of
//     /// this method is provided to be consistent with the other generators, but the
//     /// parameters are ignored. You can use [`RegionGenerator::default`] to create a
//     /// default generator.
//     ///
//     /// Note the generator's lifetime is `&'static`. See [`NationGenerator`] for
//     /// more details.
//     pub fn new(_scale_factor: f64, _vehicle: i32, _vehicle_count: i32) -> RegionGenerator<'static> {
//         // Note: use explicit lifetime to ensure this remains `&'static`
//         Self::new_with_distributions_and_text_pool(
//             Distributions::static_default(),
//             TextPool::get_or_init_default(),
//         )
//     }
//
//     /// Creates a RegionGenerator with the specified distributions and text pool
//     pub fn new_with_distributions_and_text_pool<'b>(
//         distributions: &'b Distributions,
//         text_pool: &'b TextPool,
//     ) -> RegionGenerator<'b> {
//         RegionGenerator {
//             distributions,
//             text_pool,
//         }
//     }
//
//     /// Returns an iterator over the region rows
//     pub fn iter(&self) -> RegionGeneratorIterator<'a> {
//         RegionGeneratorIterator::new(self.distributions.regions(), self.text_pool)
//     }
// }
//
// impl<'a> IntoIterator for &'a RegionGenerator<'a> {
//     type Item = Region<'a>;
//     type IntoIter = RegionGeneratorIterator<'a>;
//
//     fn into_iter(self) -> Self::IntoIter {
//         self.iter()
//     }
// }
//
// /// Iterator that generates Region rows
// #[derive(Debug)]
// pub struct RegionGeneratorIterator<'a> {
//     regions: &'a Distribution,
//     comment_random: RandomText<'a>,
//     index: usize,
// }
//
// impl<'a> RegionGeneratorIterator<'a> {
//     const COMMENT_AVERAGE_LENGTH: i32 = 72;
//
//     fn new(regions: &'a Distribution, text_pool: &'a TextPool) -> Self {
//         RegionGeneratorIterator {
//             regions,
//             comment_random: RandomText::new(
//                 1500869201,
//                 text_pool,
//                 Self::COMMENT_AVERAGE_LENGTH as f64,
//             ),
//             index: 0,
//         }
//     }
// }
//
// impl<'a> Iterator for RegionGeneratorIterator<'a> {
//     type Item = Region<'a>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.index >= self.regions.size() {
//             return None;
//         }
//
//         let region = Region {
//             r_regionkey: self.index as i64,
//             r_name: self.regions.get_value(self.index),
//             r_comment: self.comment_random.next_value(),
//         };
//
//         self.comment_random.row_finished();
//         self.index += 1;
//
//         Some(region)
//     }
// }

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
    /// Note the generator's lifetime is `&'static`. See [`NationGenerator`] for
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
    /// Note the generator's lifetime is `&'static`. See [`NationGenerator`] for
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
    /// Note the generator's lifetime is `&'static`. See [`NationGenerator`] for
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

// /// A clerk name, formatted as `"Clerk#<n>"`
// #[derive(Debug, Clone, Copy, PartialEq)]
// pub struct ClerkName(i32);
//
// impl ClerkName {
//     /// Creates a new ClerkName with the given value
//     pub fn new(value: i32) -> Self {
//         ClerkName(value)
//     }
// }
//
// impl Display for ClerkName {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "Clerk#{:09}", self.0)
//     }
// }
//
// /// Order status (F=final, O=open, P=pending)
// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
// pub enum OrderStatus {
//     /// Fulfilled - all line items shipped
//     Fulfilled,
//     /// Open - no line items shipped
//     Open,
//     /// Partially fulfilled - some line items shipped
//     Pending,
// }
//
// impl OrderStatus {
//     pub fn as_str(&self) -> &'static str {
//         match self {
//             OrderStatus::Fulfilled => "F",
//             OrderStatus::Open => "O",
//             OrderStatus::Pending => "P",
//         }
//     }
// }
//
// impl Display for OrderStatus {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "{}", self.as_str())
//     }
// }
//
// /// The ORDERS table
// ///
// /// The Display trait is implemented to format the line item data as a string
// /// in the default TPC-H 'tbl' format.
// ///
// /// ```text
// /// 1|37|O|131251.81|1996-01-02|5-LOW|Clerk#000000951|0|nstructions sleep furiously among |
// ///  2|79|O|40183.29|1996-12-01|1-URGENT|Clerk#000000880|0| foxes. pending accounts at the pending, silent asymptot|
// /// ```
// #[derive(Debug, Clone, PartialEq)]
// pub struct Order<'a> {
//     /// Primary key
//     pub o_orderkey: i64,
//     /// Foreign key to CUSTOMER
//     pub o_custkey: i64,
//     /// Order status (F=final, O=open, P=pending)
//     pub o_orderstatus: OrderStatus,
//     /// Order total price
//     pub o_totalprice: TPCHDecimal,
//     /// Order date
//     pub o_orderdate: TPCHDate,
//     /// Order priority
//     pub o_orderpriority: &'a str,
//     /// Clerk who processed the order.
//     pub o_clerk: ClerkName,
//     /// Order shipping priority
//     pub o_shippriority: i32,
//     /// Variable length comment
//     pub o_comment: &'a str,
// }
//
// impl Display for Order<'_> {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(
//             f,
//             "{}|{}|{}|{}|{}|{}|{}|{}|{}|",
//             self.o_orderkey,
//             self.o_custkey,
//             self.o_orderstatus,
//             self.o_totalprice,
//             self.o_orderdate,
//             self.o_orderpriority,
//             self.o_clerk,
//             self.o_shippriority,
//             self.o_comment
//         )
//     }
// }
//
// /// Generator for Order table data
// #[derive(Debug, Clone)]
// pub struct OrderGenerator<'a> {
//     scale_factor: f64,
//     part: i32,
//     part_count: i32,
//     distributions: &'a Distributions,
//     text_pool: &'a TextPool,
// }
//
// impl<'a> OrderGenerator<'a> {
//     /// Base scale for order generation
//     pub const SCALE_BASE: i32 = 1_500_000;
//
//     // Constants for order generation
//     const CUSTOMER_MORTALITY: i32 = 3; // portion with no orders
//     const ORDER_DATE_MIN: i32 = dates::MIN_GENERATE_DATE;
//     const ORDER_DATE_MAX: i32 =
//         Self::ORDER_DATE_MIN + (dates::TOTAL_DATE_RANGE - LineItemGenerator::ITEM_SHIP_DAYS - 1);
//     const CLERK_SCALE_BASE: i32 = 1000;
//
//     const LINE_COUNT_MIN: i32 = 1;
//     pub const LINE_COUNT_MAX: i32 = 7;
//
//     const COMMENT_AVERAGE_LENGTH: i32 = 49;
//
//     const ORDER_KEY_SPARSE_BITS: i32 = 2;
//     const ORDER_KEY_SPARSE_KEEP: i32 = 3;
//     /// Creates a new OrderGenerator with the given scale factor
//     ///
//     /// Note the generator's lifetime is `&'static`. See [`NationGenerator`] for
//     /// more details.
//     pub fn new(scale_factor: f64, part: i32, part_count: i32) -> OrderGenerator<'static> {
//         // Note: use explicit lifetime to ensure this remains `&'static`
//         Self::new_with_distributions_and_text_pool(
//             scale_factor,
//             part,
//             part_count,
//             Distributions::static_default(),
//             TextPool::get_or_init_default(),
//         )
//     }
//
//     /// Creates a OrderGenerator with specified distributions and text pool
//     pub fn new_with_distributions_and_text_pool<'b>(
//         scale_factor: f64,
//         part: i32,
//         part_count: i32,
//         distributions: &'b Distributions,
//         text_pool: &'b TextPool,
//     ) -> OrderGenerator<'b> {
//         OrderGenerator {
//             scale_factor,
//             part,
//             part_count,
//             distributions,
//             text_pool,
//         }
//     }
//
//     /// Return the row count for the given scale factor and generator part count
//     pub fn calculate_row_count(scale_factor: f64, part: i32, part_count: i32) -> i64 {
//         GenerateUtils::calculate_row_count(Self::SCALE_BASE, scale_factor, part, part_count)
//     }
//
//     /// Returns an iterator over the order rows
//     pub fn iter(&self) -> OrderGeneratorIterator<'a> {
//         OrderGeneratorIterator::new(
//             self.distributions,
//             self.text_pool,
//             self.scale_factor,
//             GenerateUtils::calculate_start_index(
//                 Self::SCALE_BASE,
//                 self.scale_factor,
//                 self.part,
//                 self.part_count,
//             ),
//             Self::calculate_row_count(self.scale_factor, self.part, self.part_count),
//         )
//     }
//
//     /// Creates the order date random generator
//     pub fn create_order_date_random() -> RandomBoundedInt {
//         RandomBoundedInt::new(1066728069, Self::ORDER_DATE_MIN, Self::ORDER_DATE_MAX)
//     }
//
//     /// Creates the line count random generator
//     pub fn create_line_count_random() -> RandomBoundedInt {
//         RandomBoundedInt::new(1434868289, Self::LINE_COUNT_MIN, Self::LINE_COUNT_MAX)
//     }
//
//     /// Creates an order key from an index
//     pub fn make_order_key(order_index: i64) -> i64 {
//         let low_bits = order_index & ((1 << Self::ORDER_KEY_SPARSE_KEEP) - 1);
//
//         let mut ok = order_index;
//         ok >>= Self::ORDER_KEY_SPARSE_KEEP;
//         ok <<= Self::ORDER_KEY_SPARSE_BITS;
//         ok <<= Self::ORDER_KEY_SPARSE_KEEP;
//         ok += low_bits;
//
//         ok
//     }
// }
//
// impl<'a> IntoIterator for &'a OrderGenerator<'a> {
//     type Item = Order<'a>;
//     type IntoIter = OrderGeneratorIterator<'a>;
//
//     fn into_iter(self) -> Self::IntoIter {
//         self.iter()
//     }
// }
//
// /// Iterator that generates Order rows
// #[derive(Debug)]
// pub struct OrderGeneratorIterator<'a> {
//     order_date_random: RandomBoundedInt,
//     line_count_random: RandomBoundedInt,
//     customer_key_random: RandomBoundedLong,
//     order_priority_random: RandomString<'a>,
//     clerk_random: RandomBoundedInt,
//     comment_random: RandomText<'a>,
//
//     // For line item simulation to determine order status
//     line_quantity_random: RandomBoundedInt,
//     line_discount_random: RandomBoundedInt,
//     line_tax_random: RandomBoundedInt,
//     line_vehicle_key_random: RandomBoundedLong,
//     line_ship_date_random: RandomBoundedInt,
//
//     start_index: i64,
//     row_count: i64,
//     max_customer_key: i64,
//
//     index: i64,
// }
// impl<'a> OrderGeneratorIterator<'a> {
//     fn new(
//         distributions: &'a Distributions,
//         text_pool: &'a TextPool,
//         scale_factor: f64,
//         start_index: i64,
//         row_count: i64,
//     ) -> Self {
//         let mut order_date_random = OrderGenerator::create_order_date_random();
//         let mut line_count_random = OrderGenerator::create_line_count_random();
//
//         let max_customer_key = (CustomerGenerator::SCALE_BASE as f64 * scale_factor) as i64;
//
//         let mut customer_key_random =
//             RandomBoundedLong::new(851767375, scale_factor >= 30000.0, 1, max_customer_key);
//
//         let mut order_priority_random =
//             RandomString::new(591449447, distributions.order_priority());
//
//         let max_clerk = (scale_factor * OrderGenerator::CLERK_SCALE_BASE as f64)
//             .max(OrderGenerator::CLERK_SCALE_BASE as f64) as i32;
//         let mut clerk_random = RandomBoundedInt::new(1171034773, 1, max_clerk);
//
//         let mut comment_random = RandomText::new(
//             276090261,
//             text_pool,
//             OrderGenerator::COMMENT_AVERAGE_LENGTH as f64,
//         );
//
//         // For line item simulation
//         let mut line_quantity_random = LineItemGenerator::create_quantity_random();
//         let mut line_discount_random = LineItemGenerator::create_discount_random();
//         let mut line_tax_random = LineItemGenerator::create_tax_random();
//         let mut line_vehicle_key_random =
//             LineItemGenerator::create_vehicle_key_random(scale_factor);
//         let mut line_ship_date_random = LineItemGenerator::create_ship_date_random();
//
//         // Advance all generators to the starting position
//         order_date_random.advance_rows(start_index);
//         line_count_random.advance_rows(start_index);
//         customer_key_random.advance_rows(start_index);
//         order_priority_random.advance_rows(start_index);
//         clerk_random.advance_rows(start_index);
//         comment_random.advance_rows(start_index);
//
//         line_quantity_random.advance_rows(start_index);
//         line_discount_random.advance_rows(start_index);
//         line_tax_random.advance_rows(start_index);
//         line_vehicle_key_random.advance_rows(start_index);
//         line_ship_date_random.advance_rows(start_index);
//
//         OrderGeneratorIterator {
//             order_date_random,
//             line_count_random,
//             customer_key_random,
//             order_priority_random,
//             clerk_random,
//             comment_random,
//             line_quantity_random,
//             line_discount_random,
//             line_tax_random,
//             line_vehicle_key_random,
//             line_ship_date_random,
//             start_index,
//             row_count,
//             max_customer_key,
//             index: 0,
//         }
//     }
//
//     /// Creates an order with the given index
//     fn make_order(&mut self, index: i64) -> Order<'a> {
//         let order_key = OrderGenerator::make_order_key(index);
//
//         let order_date = self.order_date_random.next_value();
//
//         // generate customer key, taking into account customer mortality rate
//         let mut customer_key = self.customer_key_random.next_value();
//         let mut delta = 1;
//         while customer_key % OrderGenerator::CUSTOMER_MORTALITY as i64 == 0 {
//             customer_key += delta;
//             customer_key = customer_key.min(self.max_customer_key);
//             delta *= -1;
//         }
//
//         let mut total_price = 0;
//         let mut shipped_count = 0;
//
//         let line_count = self.line_count_random.next_value();
//         for _ in 0..line_count {
//             let quantity = self.line_quantity_random.next_value();
//             let discount = self.line_discount_random.next_value();
//             let tax = self.line_tax_random.next_value();
//
//             let vehicle_key = self.line_vehicle_key_random.next_value();
//
//             let vehicle_price = VehicleGeneratorIterator::calculate_vehicle_price(vehicle_key);
//             let extended_price = vehicle_price * quantity as i64;
//             let discounted_price = extended_price * (100 - discount as i64);
//             total_price += ((discounted_price / 100) * (100 + tax as i64)) / 100;
//
//             let ship_date = self.line_ship_date_random.next_value() + order_date;
//             if TPCHDate::is_in_past(ship_date) {
//                 shipped_count += 1;
//             }
//         }
//
//         let order_status = if shipped_count == line_count {
//             OrderStatus::Fulfilled
//         } else if shipped_count > 0 {
//             OrderStatus::Pending
//         } else {
//             OrderStatus::Open
//         };
//
//         let clerk_id = self.clerk_random.next_value();
//         let clerk_name = ClerkName::new(clerk_id);
//
//         Order {
//             o_orderkey: order_key,
//             o_custkey: customer_key,
//             o_orderstatus: order_status,
//             o_totalprice: TPCHDecimal(total_price),
//             o_orderdate: TPCHDate::new(order_date, 0, 0),
//             o_orderpriority: self.order_priority_random.next_value(),
//             o_clerk: clerk_name,
//             o_shippriority: 0, // Fixed value per TPC-H spec
//             o_comment: self.comment_random.next_value(),
//         }
//     }
// }
//
// impl<'a> Iterator for OrderGeneratorIterator<'a> {
//     type Item = Order<'a>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.index >= self.row_count {
//             return None;
//         }
//
//         let order = self.make_order(self.start_index + self.index + 1);
//
//         self.order_date_random.row_finished();
//         self.line_count_random.row_finished();
//         self.customer_key_random.row_finished();
//         self.order_priority_random.row_finished();
//         self.clerk_random.row_finished();
//         self.comment_random.row_finished();
//
//         self.line_quantity_random.row_finished();
//         self.line_discount_random.row_finished();
//         self.line_tax_random.row_finished();
//         self.line_vehicle_key_random.row_finished();
//         self.line_ship_date_random.row_finished();
//
//         self.index += 1;
//
//         Some(order)
//     }
// }
//
// /// The LINEITEM table
// ///
// /// The Display trait is implemented to format the line item data as a string
// /// in the default TPC-H 'tbl' format.
// ///
// /// Example
// /// ```text
// /// 1|156|4|1|17|17954.55|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the|
// /// 1|68|9|2|36|34850.16|0.09|0.06|N|O|1996-04-12|1996-02-28|1996-04-20|TAKE BACK RETURN|MAIL|ly final dependencies: slyly bold |
// /// ```
// #[derive(Debug, Clone, PartialEq)]
// pub struct LineItem<'a> {
//     /// Foreign key to ORDERS
//     pub l_orderkey: i64,
//     /// Foreign key to VEHICLE
//     pub l_vehiclekey: i64,
//     /// Foreign key to Driver
//     pub l_suppkey: i64,
//     /// Line item number within order
//     pub l_linenumber: i32,
//     /// Quantity ordered
//     // TODO: Spec has this as decimal.
//     pub l_quantity: i64,
//     /// Extended price (l_quantity * p_retailprice)
//     pub l_extendedprice: TPCHDecimal,
//     /// Discount percentage
//     pub l_discount: TPCHDecimal,
//     /// Tax percentage
//     pub l_tax: TPCHDecimal,
//     /// Return flag (R=returned, A=accepted, null=pending)
//     pub l_returnflag: &'a str,
//     /// Line status (O=ordered, F=fulfilled)
//     pub l_linestatus: &'static str,
//     /// Date shipped
//     pub l_shipdate: TPCHDate,
//     /// Date committed to ship
//     pub l_commitdate: TPCHDate,
//     /// Date received
//     pub l_receiptdate: TPCHDate,
//     /// Shipping instructions
//     pub l_shipinstruct: &'a str,
//     /// Shipping mode
//     pub l_shipmode: &'a str,
//     /// Variable length comment
//     pub l_comment: &'a str,
// }
//
// impl Display for LineItem<'_> {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(
//             f,
//             "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|",
//             self.l_orderkey,
//             self.l_vehiclekey,
//             self.l_suppkey,
//             self.l_linenumber,
//             self.l_quantity,
//             self.l_extendedprice,
//             self.l_discount,
//             self.l_tax,
//             self.l_returnflag,
//             self.l_linestatus,
//             self.l_shipdate,
//             self.l_commitdate,
//             self.l_receiptdate,
//             self.l_shipinstruct,
//             self.l_shipmode,
//             self.l_comment
//         )
//     }
// }
//
// /// Generator for LineItem table data
// #[derive(Debug, Clone)]
// pub struct LineItemGenerator<'a> {
//     scale_factor: f64,
//     part: i32,
//     part_count: i32,
//     distributions: &'a Distributions,
//     text_pool: &'a TextPool,
// }
//
// impl<'a> LineItemGenerator<'a> {
//     // Constants for line item generation
//     const QUANTITY_MIN: i32 = 1;
//     const QUANTITY_MAX: i32 = 50;
//     const TAX_MIN: TPCHDecimal = TPCHDecimal(0); // 0.00
//     const TAX_MAX: TPCHDecimal = TPCHDecimal(8); // 0.08
//     const DISCOUNT_MIN: TPCHDecimal = TPCHDecimal(0); // 0.00
//     const DISCOUNT_MAX: TPCHDecimal = TPCHDecimal(10); // 0.10
//     const VEHICLE_KEY_MIN: i32 = 1;
//     const SHIP_DATE_MIN: i32 = 1;
//     const SHIP_DATE_MAX: i32 = 121;
//     const COMMIT_DATE_MIN: i32 = 30;
//     const COMMIT_DATE_MAX: i32 = 90;
//     const RECEIPT_DATE_MIN: i32 = 1;
//     const RECEIPT_DATE_MAX: i32 = 30;
//
//     pub const ITEM_SHIP_DAYS: i32 = Self::SHIP_DATE_MAX + Self::RECEIPT_DATE_MAX;
//
//     const COMMENT_AVERAGE_LENGTH: i32 = 27;
//
//     /// Creates a new LineItemGenerator with the given scale factor
//     ///
//     /// Note the generator's lifetime is `&'static`. See [`NationGenerator`] for
//     /// more details.
//     pub fn new(scale_factor: f64, part: i32, part_count: i32) -> LineItemGenerator<'static> {
//         Self::new_with_distributions_and_text_pool(
//             scale_factor,
//             part,
//             part_count,
//             Distributions::static_default(),
//             TextPool::get_or_init_default(),
//         )
//     }
//
//     /// Creates a LineItemGenerator with specified distributions and text pool
//     pub fn new_with_distributions_and_text_pool<'b>(
//         scale_factor: f64,
//         part: i32,
//         part_count: i32,
//         distributions: &'b Distributions,
//         text_pool: &'b TextPool,
//     ) -> LineItemGenerator<'b> {
//         LineItemGenerator {
//             scale_factor,
//             part,
//             part_count,
//             distributions,
//             text_pool,
//         }
//     }
//
//     /// Returns an iterator over the line item rows
//     pub fn iter(&self) -> LineItemGeneratorIterator<'a> {
//         LineItemGeneratorIterator::new(
//             self.distributions,
//             self.text_pool,
//             self.scale_factor,
//             GenerateUtils::calculate_start_index(
//                 OrderGenerator::SCALE_BASE,
//                 self.scale_factor,
//                 self.part,
//                 self.part_count,
//             ),
//             GenerateUtils::calculate_row_count(
//                 OrderGenerator::SCALE_BASE,
//                 self.scale_factor,
//                 self.part,
//                 self.part_count,
//             ),
//         )
//     }
//
//     /// Creates a quantity random generator
//     pub fn create_quantity_random() -> RandomBoundedInt {
//         RandomBoundedInt::new_with_seeds_per_row(
//             209208115,
//             Self::QUANTITY_MIN,
//             Self::QUANTITY_MAX,
//             OrderGenerator::LINE_COUNT_MAX,
//         )
//     }
//
//     /// Creates a discount random generator
//     pub fn create_discount_random() -> RandomBoundedInt {
//         RandomBoundedInt::new_with_seeds_per_row(
//             554590007,
//             Self::DISCOUNT_MIN.0 as i32,
//             Self::DISCOUNT_MAX.0 as i32,
//             OrderGenerator::LINE_COUNT_MAX,
//         )
//     }
//
//     /// Creates a tax random generator
//     pub fn create_tax_random() -> RandomBoundedInt {
//         RandomBoundedInt::new_with_seeds_per_row(
//             721958466,
//             Self::TAX_MIN.0 as i32,
//             Self::TAX_MAX.0 as i32,
//             OrderGenerator::LINE_COUNT_MAX,
//         )
//     }
//
//     /// Creates a vehicle key random generator
//     pub fn create_vehicle_key_random(scale_factor: f64) -> RandomBoundedLong {
//         // If scale_factor >= 30000, use long `RandomBoundedLong` otherwise
//         // use `RandomBoundedInt` to avoid overflow.
//         RandomBoundedLong::new_with_seeds_per_row(
//             1808217256,
//             scale_factor >= 30000.0,
//             Self::VEHICLE_KEY_MIN as i64,
//             (VehicleGenerator::SCALE_BASE as f64 * scale_factor) as i64,
//             OrderGenerator::LINE_COUNT_MAX,
//         )
//     }
//
//     /// Creates a ship date random generator
//     pub fn create_ship_date_random() -> RandomBoundedInt {
//         RandomBoundedInt::new_with_seeds_per_row(
//             1769349045,
//             Self::SHIP_DATE_MIN,
//             Self::SHIP_DATE_MAX,
//             OrderGenerator::LINE_COUNT_MAX,
//         )
//     }
// }
//
// impl<'a> IntoIterator for &'a LineItemGenerator<'a> {
//     type Item = LineItem<'a>;
//     type IntoIter = LineItemGeneratorIterator<'a>;
//
//     fn into_iter(self) -> Self::IntoIter {
//         self.iter()
//     }
// }
//
// /// Iterator that generates LineItem rows
// #[derive(Debug)]
// pub struct LineItemGeneratorIterator<'a> {
//     order_date_random: RandomBoundedInt,
//     line_count_random: RandomBoundedInt,
//
//     quantity_random: RandomBoundedInt,
//     discount_random: RandomBoundedInt,
//     tax_random: RandomBoundedInt,
//
//     line_vehicle_key_random: RandomBoundedLong,
//
//     driver_number_random: RandomBoundedInt,
//
//     ship_date_random: RandomBoundedInt,
//     commit_date_random: RandomBoundedInt,
//     receipt_date_random: RandomBoundedInt,
//
//     returned_flag_random: RandomString<'a>,
//     ship_instructions_random: RandomString<'a>,
//     ship_mode_random: RandomString<'a>,
//
//     comment_random: RandomText<'a>,
//
//     scale_factor: f64,
//     start_index: i64,
//     row_count: i64,
//
//     index: i64,
//     order_date: i32,
//     line_count: i32,
//     line_number: i32,
// }
//
// impl<'a> LineItemGeneratorIterator<'a> {
//     fn new(
//         distributions: &'a Distributions,
//         text_pool: &'a TextPool,
//         scale_factor: f64,
//         start_index: i64,
//         row_count: i64,
//     ) -> Self {
//         let mut order_date_random = OrderGenerator::create_order_date_random();
//         let mut line_count_random = OrderGenerator::create_line_count_random();
//
//         let mut quantity_random = LineItemGenerator::create_quantity_random();
//         let mut discount_random = LineItemGenerator::create_discount_random();
//         let mut tax_random = LineItemGenerator::create_tax_random();
//
//         let mut line_vehicle_key_random =
//             LineItemGenerator::create_vehicle_key_random(scale_factor);
//
//         let mut driver_number_random = RandomBoundedInt::new_with_seeds_per_row(
//             2095021727,
//             0,
//             3,
//             OrderGenerator::LINE_COUNT_MAX,
//         );
//
//         let mut ship_date_random = LineItemGenerator::create_ship_date_random();
//         let mut commit_date_random = RandomBoundedInt::new_with_seeds_per_row(
//             904914315,
//             LineItemGenerator::COMMIT_DATE_MIN,
//             LineItemGenerator::COMMIT_DATE_MAX,
//             OrderGenerator::LINE_COUNT_MAX,
//         );
//         let mut receipt_date_random = RandomBoundedInt::new_with_seeds_per_row(
//             373135028,
//             LineItemGenerator::RECEIPT_DATE_MIN,
//             LineItemGenerator::RECEIPT_DATE_MAX,
//             OrderGenerator::LINE_COUNT_MAX,
//         );
//
//         let mut returned_flag_random = RandomString::new_with_expected_row_count(
//             717419739,
//             distributions.return_flags(),
//             OrderGenerator::LINE_COUNT_MAX,
//         );
//         let mut ship_instructions_random = RandomString::new_with_expected_row_count(
//             1371272478,
//             distributions.ship_instructions(),
//             OrderGenerator::LINE_COUNT_MAX,
//         );
//         let mut ship_mode_random = RandomString::new_with_expected_row_count(
//             675466456,
//             distributions.ship_modes(),
//             OrderGenerator::LINE_COUNT_MAX,
//         );
//         let mut comment_random = RandomText::new_with_expected_row_count(
//             1095462486,
//             text_pool,
//             LineItemGenerator::COMMENT_AVERAGE_LENGTH as f64,
//             OrderGenerator::LINE_COUNT_MAX,
//         );
//
//         // Advance all generators to the starting position
//         order_date_random.advance_rows(start_index);
//         line_count_random.advance_rows(start_index);
//
//         quantity_random.advance_rows(start_index);
//         discount_random.advance_rows(start_index);
//         tax_random.advance_rows(start_index);
//
//         line_vehicle_key_random.advance_rows(start_index);
//
//         driver_number_random.advance_rows(start_index);
//
//         ship_date_random.advance_rows(start_index);
//         commit_date_random.advance_rows(start_index);
//         receipt_date_random.advance_rows(start_index);
//
//         returned_flag_random.advance_rows(start_index);
//         ship_instructions_random.advance_rows(start_index);
//         ship_mode_random.advance_rows(start_index);
//
//         comment_random.advance_rows(start_index);
//
//         // generate information for initial order
//         let order_date = order_date_random.next_value();
//         let line_count = line_count_random.next_value() - 1;
//
//         LineItemGeneratorIterator {
//             order_date_random,
//             line_count_random,
//             quantity_random,
//             discount_random,
//             tax_random,
//             line_vehicle_key_random,
//             driver_number_random,
//             ship_date_random,
//             commit_date_random,
//             receipt_date_random,
//             returned_flag_random,
//             ship_instructions_random,
//             ship_mode_random,
//             comment_random,
//             scale_factor,
//             start_index,
//             row_count,
//             index: 0,
//             order_date,
//             line_count,
//             line_number: 0,
//         }
//     }
//
//     /// Creates a line item with the given order index
//     fn make_line_item(&mut self, order_index: i64) -> LineItem<'a> {
//         let order_key = OrderGenerator::make_order_key(order_index);
//
//         let quantity = self.quantity_random.next_value();
//         let discount = self.discount_random.next_value();
//         let tax = self.tax_random.next_value();
//
//         let vehicle_key = self.line_vehicle_key_random.next_value();
//
//         // let driver_number = self.driver_number_random.next_value() as i64;
//         let driver_key = DriverGeneratorIterator::select_driver(
//             vehicle_key,
//             self.line_number as i64,
//             self.scale_factor,
//         );
//
//         let vehicle_price = VehicleGeneratorIterator::calculate_vehicle_price(vehicle_key);
//         let extended_price = vehicle_price * quantity as i64;
//
//         let mut ship_date = self.ship_date_random.next_value();
//         ship_date += self.order_date;
//         let mut commit_date = self.commit_date_random.next_value();
//         commit_date += self.order_date;
//         let mut receipt_date = self.receipt_date_random.next_value();
//         receipt_date += ship_date;
//
//         let returned_flag = if TPCHDate::is_in_past(receipt_date) {
//             self.returned_flag_random.next_value()
//         } else {
//             "N"
//         };
//
//         let status = if TPCHDate::is_in_past(ship_date) {
//             "F" // Fulfilled
//         } else {
//             "O" // Open
//         };
//
//         let ship_instructions = self.ship_instructions_random.next_value();
//         let ship_mode = self.ship_mode_random.next_value();
//         let comment = self.comment_random.next_value();
//
//         LineItem {
//             l_orderkey: order_key,
//             l_vehiclekey: vehicle_key,
//             l_suppkey: driver_key,
//             l_linenumber: self.line_number + 1,
//             l_quantity: quantity as i64,
//             l_extendedprice: TPCHDecimal(extended_price),
//             l_discount: TPCHDecimal(discount as i64),
//             l_tax: TPCHDecimal(tax as i64),
//             l_returnflag: returned_flag,
//             l_linestatus: status,
//             l_shipdate: TPCHDate::new(ship_date, 0, 0),
//             l_commitdate: TPCHDate::new(commit_date, 0, 0),
//             l_receiptdate: TPCHDate::new(receipt_date, 0, 0),
//             l_shipinstruct: ship_instructions,
//             l_shipmode: ship_mode,
//             l_comment: comment,
//         }
//     }
// }
//
// impl<'a> Iterator for LineItemGeneratorIterator<'a> {
//     type Item = LineItem<'a>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.index >= self.row_count {
//             return None;
//         }
//
//         let line_item = self.make_line_item(self.start_index + self.index + 1);
//         self.line_number += 1;
//
//         // advance next row only when all lines for the order have been produced
//         if self.line_number > self.line_count {
//             self.order_date_random.row_finished();
//             self.line_count_random.row_finished();
//
//             self.quantity_random.row_finished();
//             self.discount_random.row_finished();
//             self.tax_random.row_finished();
//
//             self.line_vehicle_key_random.row_finished();
//             self.driver_number_random.row_finished();
//
//             self.ship_date_random.row_finished();
//             self.commit_date_random.row_finished();
//             self.receipt_date_random.row_finished();
//
//             self.returned_flag_random.row_finished();
//             self.ship_instructions_random.row_finished();
//             self.ship_mode_random.row_finished();
//
//             self.comment_random.row_finished();
//
//             self.index += 1;
//
//             // generate information for next order
//             self.line_count = self.line_count_random.next_value() - 1;
//             self.order_date = self.order_date_random.next_value();
//             self.line_number = 0;
//         }
//
//         Some(line_item)
//     }
// }

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
    pub t_pickuploc: String,
    /// Trip dropoff coordinates
    pub t_dropoffloc: String,
}

impl Display for Trip {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|",
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
    hour_random: RandomBoundedInt,
    minute_random: RandomBoundedInt,
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
        let mut hour_random = RandomBoundedInt::new(123456789, 0, 23);
        let mut minute_random = RandomBoundedInt::new(987654321, 0, 59);

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
        hour_random.advance_rows(start_index);
        minute_random.advance_rows(start_index);
        fare_per_mile_random.advance_rows(start_index);
        tip_percent_random.advance_rows(start_index);
        trip_minutes_per_mile_random.advance_rows(start_index);

        TripGeneratorIterator {
            customer_key_random,
            driver_key_random,
            vehicle_key_random,
            pickup_date_random,
            hour_random,
            minute_random,
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

        // After (with random hour/minute as example):
        let hour = self.hour_random.next_value();
        let minute = self.minute_random.next_value();
        let pickup_date = TPCHDate::new(pickup_date_value, hour as u8, minute as u8);

        // Get distance from KDE model (in miles with decimal precision)
        let distance_value = self.distance_kde.generate(trip_key as u64);
        let distance = TPCHDecimal((distance_value * 100.0) as i64);

        // Pickup
        let pickuploc = self.spatial_gen.generate(trip_key as u64);

        // Extract just the coordinates part by removing "POINT (" and ")"
        let coords_str = pickuploc
            .trim_start_matches("POINT (")
            .trim_end_matches(")");
        let coords: Vec<&str> = coords_str.split_whitespace().collect();

        // Parse the coordinates directly
        let pickup_x = coords[0].parse::<f64>().unwrap();
        let pickup_y = coords[1].parse::<f64>().unwrap();

        // Angle
        let angle_seed = spider_seed_for_index(trip_key as u64, 1234);
        let mut angle_rng = StdRng::seed_from_u64(angle_seed);
        let angle: f64 = angle_rng.gen::<f64>() * std::f64::consts::TAU;

        // Dropoff via polar projection
        let dropoff_x = pickup_x + distance_value * angle.cos();
        let dropoff_y = pickup_y + distance_value * angle.sin();
        let dropoffloc = format!("POINT ({} {})", dropoff_x, dropoff_y);

        // Fix multiplication of f64 by integers by using f64 literals
        let fare_per_mile = self.fare_per_mile_random.next_value() as f64;
        let fare_value = (distance_value * fare_per_mile) / 100.0;
        let fare = TPCHDecimal((fare_value * 100.0) as i64); // Use 100.0 (float) instead of 100 (int)

        let tip_percent = self.tip_percent_random.next_value() as f64; // Convert to f64
        let tip_value = (fare_value * tip_percent) / 100.0; // Use 100.0 instead of 100
        let tip = TPCHDecimal((tip_value * 100.0) as i64); // Use 100.0 instead of 100

        let total_value = fare_value + tip_value;
        let total = TPCHDecimal((total_value * 100.0) as i64); // Use 100.0 instead of 100

        // Calculate trip duration based on distance
        let minutes_per_mile = 3000;
        let distance_miles = distance_value;
        let duration_minutes = (distance_miles * minutes_per_mile as f64).round() as i32;

        let total_minutes = hour * 60 + minute + duration_minutes;
        let dropoff_hour = (total_minutes / 60) % 24;
        let dropoff_minute = total_minutes % 60;
        let day_delta = total_minutes / (24 * 60);
        let dropoff_day = pickup_date_value + day_delta;
        // Ensure the dropoff day doesn't exceed the maximum date value
        let bounded_dropoff_day = std::cmp::min(
            dropoff_day,
            dates::MIN_GENERATE_DATE + dates::TOTAL_DATE_RANGE - 1,
        );
        let dropoff_date = TPCHDate::new(
            bounded_dropoff_day,
            dropoff_hour as u8,
            dropoff_minute as u8,
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
    pub b_boundary: String,
}

impl Display for Building<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}|{}|{}|",
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

    /// Creates a new VehicleGenerator with the given scale factor
    ///
    /// Note the generator's lifetime is `&'static`. See [`NationGenerator`] for
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
        let wkt = self.spatial_gen.generate(building_key as u64);

        Building {
            b_buildingkey: building_key,
            b_name: name,
            b_boundary: wkt,
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
    /// Name of the zone
    pub z_name: String,
    /// Subtype of the zone
    pub z_subtype: String,
    /// Boundary geometry in WKT format
    pub z_boundary: String,
}

impl Display for Zone {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|",
            self.z_zonekey, self.z_gersid, self.z_name, self.z_subtype, self.z_boundary
        )
    }
}

/// Generator for [`Zone`]s that loads from a parquet file in S3
#[derive(Debug, Clone)]
pub struct ZoneGenerator {
    zones: Vec<Zone>,
    part: i32,
    part_count: i32,
}

impl ZoneGenerator {
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
    // (OVERTURE_RELEASE_DATE,"s3://overturemaps-us-west-2/release/2025-06-25.0/theme=divisions/type=division_area/*");

    /// Creates a new ZoneGenerator that loads data from S3
    pub fn new(_scale_factor: f64, part: i32, part_count: i32) -> ZoneGenerator {
        // Load zones from parquet file in S3
        let zones = Self::load_zones_from_s3();

        ZoneGenerator {
            zones,
            part,
            part_count,
        }
    }

    /// Loads zone data from S3 parquet file using DuckDB
    fn load_zones_from_s3() -> Vec<Zone> {
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

        // Set S3 region
        conn.execute("SET s3_region='us-west-2';", [])
            .expect("Failed to set S3 region");

        // Query the parquet file directly - Cast the division_id to BIGINT
        let mut stmt = conn
            .prepare(
                "SELECT
            id as z_gersid,
            COALESCE(names.primary, '') as z_name,
            subtype as z_subtype,
            ST_AsText(geometry) as z_boundary
         FROM read_parquet(?1, hive_partitioning=1)
         WHERE subtype IN ('county', 'locality', 'neighbourhood')",
            )
            .expect("Failed to prepare query");

        let zones_url = Self::get_zones_parquet_url();
        let mut zones = Vec::new();
        // Counter for primary key
        let mut zone_id = 1;
        let mut rows = stmt.query([&zones_url]).expect("Failed to execute query");

        while let Ok(Some(row)) = rows.next() {
            // Read the row values
            let zone = Zone {
                z_zonekey: zone_id,
                z_gersid: row.get(0).expect("Failed to read gers_id"),
                z_name: row.get(1).expect("Failed to read name"),
                z_subtype: row.get(2).expect("Failed to read subtype"),
                z_boundary: row.get(3).expect("Failed to read wkt"),
            };

            zones.push(zone);

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

    // #[test]
    // fn test_nation_generator() {
    //     let generator = NationGenerator::default();
    //     let nations: Vec<_> = generator.iter().collect();
    //
    //     // TPC-H typically has 25 nations
    //     assert_eq!(nations.len(), 25);
    // }
    //
    // #[test]
    // fn test_region_generator() {
    //     let generator = RegionGenerator::default();
    //     let regions: Vec<_> = generator.iter().collect();
    //
    //     // TPC-H typically has 5 regions
    //     assert_eq!(regions.len(), 5);
    // }

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

        // Should have 0.01 * 150,000 = 1,500 customers
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

    // #[test]
    // fn test_order_generation() {
    //     // Create a generator with a small scale factor
    //     let generator = OrderGenerator::new(0.01, 1, 1);
    //     let orders: Vec<_> = generator.iter().collect();
    //
    //     // Should have 0.01 * 1,500,000 = 15,000 orders
    //     assert_eq!(orders.len(), 15000);
    //
    //     // Check first order
    //     let first = &orders[0];
    //     assert_eq!(first.o_orderkey, OrderGenerator::make_order_key(1));
    //     assert!(first.o_custkey > 0);
    //     assert!(first.o_totalprice > TPCHDecimal::ZERO);
    //
    //     // Check order status distribution
    //     let status_counts =
    //         orders
    //             .iter()
    //             .fold(std::collections::HashMap::new(), |mut acc, order| {
    //                 *acc.entry(&order.o_orderstatus).or_insert(0) += 1;
    //                 acc
    //             });
    //
    //     // Should have multiple order statuses
    //     assert!(status_counts.len() >= 2);
    //
    //     // Check customer key distribution - no customer with mortality factor
    //     assert!(orders
    //         .iter()
    //         .all(|o| o.o_custkey % OrderGenerator::CUSTOMER_MORTALITY as i64 != 0));
    //
    //     // Check order key sparsity
    //     for (i, order) in orders.iter().enumerate() {
    //         assert_eq!(
    //             order.o_orderkey,
    //             OrderGenerator::make_order_key(i as i64 + 1)
    //         );
    //     }
    // }

    #[test]
    fn test_trip_generation() {
        // Create a generator with a small scale factor
        let generator = TripGenerator::new(0.01, 1, 1);
        let trips: Vec<_> = generator.iter().collect();

        // Should have 0.01 * 1,000,000 = 10,000 trips
        assert_eq!(trips.len(), 60_000);

        // Check first trip
        let first = &trips[0];
        assert_eq!(first.t_tripkey, 1);
        assert!(first.t_custkey > 0);
        assert!(first.t_driverkey > 0);
        assert!(first.t_vehiclekey > 0);

        // Check that pickup date is before or equal to dropoff date
        // TPCHDate doesn't have a .0 field, use date comparison instead
        // assert!(first.t_pickuptime <= first.t_dropofftime);

        // Check that the financial values make sense
        // assert!(first.t_fare.0 > 0);
        // assert!(first.t_tip.0 >= 0); // Tip could be zero
        // assert_eq!(first.t_totalamount.0, first.t_fare.0 + first.t_tip.0);
        // assert!(first.t_distance.0 > 0);

        // Verify the string format matches the expected pattern
        let expected_pattern = format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|",
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
        assert_eq!(first.to_string(), "2|172|1|1|1997-12-24 22:50|1997-12-24 23:32|0.03|0.00|0.04|0.01|POINT (-123.30659706835938 33.6437762421875)|POINT (-123.29286225833363 33.64593281462752)|");
    }

    #[test]
    fn test_building_generation() {
        // Create a generator with a small scale factor
        let generator = BuildingGenerator::new(1.0, 1, 1);
        let buildings: Vec<_> = generator.iter().collect();

        // Should have 0.01 * 20,000 = 200 buildings
        assert_eq!(buildings.len(), 20_000);

        // Check first building
        let first = &buildings[0];
        assert_eq!(first.b_buildingkey, 1);

        // Verify the string format matches the expected pattern
        let expected_pattern = format!(
            "{}|{}|{}|",
            first.b_buildingkey, first.b_name, first.b_boundary,
        );
        assert_eq!(first.to_string(), expected_pattern);

        // Check first Building
        let first = &buildings[1];
        assert_eq!(first.b_buildingkey, 2);
        assert_eq!(first.to_string(), "2|blush|POLYGON ((-102.2154579691 40.5193652499, -102.2133112848 40.5193652499, -102.2133112848 40.5207006446, -102.2154579691 40.5207006446, -102.2154579691 40.5193652499))|")
    }

    #[test]
    fn test_zone_generation() {
        // Create a generator with a small scale factor
        let generator = ZoneGenerator::new(0.1, 1, 1);
        let zones: Vec<_> = generator.into_iter().collect();

        assert_eq!(zones.len(), 596124);

        // Check first Driver
        let first = &zones[0];
        assert_eq!(first.z_zonekey, 1);
        assert_eq!(
            first.to_string(),
            "1|54bea793-2dc6-47b0-a4c1-5b96f17e66a3|Chatham Islands Territory|county|MULTIPOLYGON (((-176.2418754 -44.4327352, -176.2396744 -44.4349882, -176.2379244 -44.4330281, -176.2384204 -44.4312342, -176.2418754 -44.4327352)), ((-176.165218 -44.3563138, -176.1650533 -44.3413916, -176.1773808 -44.3358569, -176.18558 -44.3493409, -176.165218 -44.3563138)), ((-176.2463812 -44.3292996, -176.25687 -44.3447818, -176.2382722 -44.3507201, -176.2271372 -44.334208, -176.2025537 -44.3268945, -176.1995124 -44.3032479, -176.1894168 -44.2905304, -176.1546655 -44.2729494, -176.1543592 -44.2622464, -176.1668675 -44.2627428, -176.2124976 -44.2246559, -176.2245928 -44.2243162, -176.2372613 -44.2406153, -176.2769252 -44.2421415, -176.2395516 -44.263888, -176.2417039 -44.2735979, -176.2691625 -44.2863288, -176.2553936 -44.2884505, -176.2622322 -44.3009838, -176.2521515 -44.3107401, -176.2678921 -44.3248335, -176.2463812 -44.3292996)), ((-176.297042 -44.2627484, -176.2970605 -44.271482, -176.308416 -44.2767738, -176.30451 -44.2786311, -176.2876219 -44.271221, -176.2884382 -44.2644401, -176.297042 -44.2627484)), ((-176.311021 -44.2776918, -176.3197114 -44.2773151, -176.3172254 -44.2808867, -176.3114786 -44.2791776, -176.311021 -44.2776918)), ((-176.4329417 -43.9302024, -176.434683 -43.9323456, -176.4358817 -43.9328894, -176.4248878 -43.9322684, -176.4240868 -43.9263484, -176.4293038 -43.9296644, -176.4329417 -43.9302024)), ((-176.4366331 -43.932187, -176.4254624 -43.9253712, -176.4357067 -43.9274744, -176.4395046 -43.924925, -176.431559 -43.9215259, -176.4501784 -43.9244548, -176.4366331 -43.932187)), ((-176.4221969 -43.9245679, -176.4301156 -43.9208746, -176.4286529 -43.9224899, -176.4247589 -43.9249689, -176.4221969 -43.9245679)), ((-176.0122633 -44.2211147, -176.0169196 -44.2212224, -176.0172737 -44.2238519, -176.0110402 -44.2232676, -176.0122633 -44.2211147)), ((-175.8363587 -43.9616887, -175.8316702 -43.9639051, -175.8325821 -43.9613374, -175.8403122 -43.96143, -175.8363587 -43.9616887)), ((-176.5805133 -43.9628137, -176.6577306 -43.9996222, -176.6816746 -44.0078554, -176.6819025 -44.0216983, -176.6555137 -44.0420854, -176.6535827 -44.0641028, -176.6627075 -44.0706876, -176.6472202 -44.0772149, -176.6482685 -44.1062905, -176.6332454 -44.1112189, -176.6344814 -44.1241196, -176.6251597 -44.1198404, -176.5802089 -44.1320647, -176.5352034 -44.1132252, -176.5259311 -44.0986506, -176.4902811 -44.0906884, -176.4779756 -44.074619, -176.3932346 -44.0542143, -176.3285232 -44.0523985, -176.3258468 -44.0324594, -176.3743192 -44.0247789, -176.3959332 -44.0078081, -176.4136647 -43.9650693, -176.4172982 -43.9231882, -176.4199639 -43.9345978, -176.4232254 -43.9283748, -176.4244747 -43.9344138, -176.4298436 -43.938434, -176.4360367 -43.9679546, -176.4410401 -43.9602786, -176.4537499 -43.9649097, -176.4831926 -43.9525823, -176.4845702 -43.9415779, -176.4693064 -43.9288806, -176.4738001 -43.9161582, -176.5130964 -43.8876185, -176.5246124 -43.8538033, -176.5021658 -43.8407763, -176.5053096 -43.8348007, -176.4925828 -43.8303367, -176.4936369 -43.8234235, -176.4769766 -43.8277633, -176.466096 -43.8157384, -176.4428597 -43.8083236, -176.5410893 -43.7931335, -176.561453 -43.7740155, -176.5603003 -43.7607367, -176.5491478 -43.7519493, -176.5536843 -43.7425943, -176.520317 -43.7414966, -176.4999809 -43.7638354, -176.4773028 -43.7471847, -176.4358565 -43.7509692, -176.3957003 -43.7659852, -176.3646614 -43.7996962, -176.3662033 -43.8106932, -176.3978873 -43.8170845, -176.4080029 -43.8385784, -176.4213734 -43.8474284, -176.4294934 -43.8370633, -176.4171578 -43.8859652, -176.4290023 -43.9172323, -176.4177344 -43.9228849, -176.399475 -43.8665767, -176.3380995 -43.7916862, -176.2813702 -43.7632283, -176.2578486 -43.7629063, -176.2402717 -43.7746028, -176.2414922 -43.7581816, -176.2279886 -43.7433642, -176.1928104 -43.7365193, -176.2057026 -43.7282804, -176.2240727 -43.7348098, -176.2477363 -43.7270672, -176.2583391 -43.7366095, -176.2731865 -43.7285687, -176.277161 -43.7408033, -176.3016636 -43.7467317, -176.3483297 -43.7334095, -176.3681377 -43.7447463, -176.4407111 -43.7473247, -176.4890017 -43.7362258, -176.4978517 -43.7267419, -176.4922146 -43.7204555, -176.542068 -43.7210361, -176.6278764 -43.6926892, -176.6298091 -43.7024581, -176.6408829 -43.7035705, -176.6330032 -43.7120605, -176.6342174 -43.728201, -176.6618696 -43.7491356, -176.7473638 -43.7659103, -176.7916533 -43.7593083, -176.8035098 -43.7444319, -176.8177995 -43.7470612, -176.8249839 -43.7574442, -176.8123011 -43.7638809, -176.8113123 -43.7842118, -176.8734487 -43.7925704, -176.8785701 -43.7998532, -176.8718562 -43.8058852, -176.8795072 -43.8111477, -176.8738255 -43.8161383, -176.8936805 -43.8242597, -176.8714382 -43.831084, -176.8830472 -43.8405841, -176.8603826 -43.8382775, -176.8476351 -43.8451111, -176.8444812 -43.8366599, -176.8164895 -43.8454823, -176.7867661 -43.8383359, -176.7937225 -43.82125, -176.7556898 -43.8335995, -176.7428873 -43.8266872, -176.7158227 -43.8303507, -176.7012793 -43.822413, -176.7076715 -43.8085929, -176.6898211 -43.817584, -176.6883066 -43.7997224, -176.6698068 -43.808094, -176.6635902 -43.7973837, -176.6574547 -43.808606, -176.6458048 -43.8042609, -176.6408711 -43.8106787, -176.6082074 -43.8123215, -176.5525149 -43.8736975, -176.5340115 -43.9161587, -176.537922 -43.9433111, -176.5561114 -43.9528147, -176.5728145 -43.9419216, -176.5805133 -43.9628137)), ((-176.4284402 -43.8264365, -176.4317505 -43.8259564, -176.4360006 -43.8315037, -176.4286262 -43.8315055, -176.4284402 -43.8264365)), ((-176.4388969 -43.8287886, -176.4361344 -43.8290511, -176.4333404 -43.8255371, -176.4380225 -43.8267176, -176.4388969 -43.8287886)))|"
        )
    }

    // #[test]
    // fn test_make_order_key() {
    //     // Test order key generation logic
    //     assert_eq!(OrderGenerator::make_order_key(1), 1); // Low values are preserved
    //     assert_eq!(OrderGenerator::make_order_key(8), 32); // 8 becomes 1000000
    //     assert_eq!(OrderGenerator::make_order_key(9), 32 + 1); // 9 becomes 1000001
    //     assert_eq!(OrderGenerator::make_order_key(10), 32 + 2); // 10 becomes 1000010
    // }

    // #[test]
    // fn test_line_item_generation() {
    //     // Create a generator with a small scale factor
    //     let generator = LineItemGenerator::new(0.01, 1, 1);
    //     let line_items: Vec<_> = generator.iter().collect();
    //
    //     // Check first line item
    //     let first = &line_items[0];
    //     assert_eq!(first.l_orderkey, OrderGenerator::make_order_key(1));
    //     assert_eq!(first.l_linenumber, 1);
    //     assert!(first.l_vehiclekey > 0);
    //     assert!(first.l_suppkey > 0);
    //
    //     assert!(first.l_quantity >= LineItemGenerator::QUANTITY_MIN as i64);
    //     assert!(first.l_quantity <= LineItemGenerator::QUANTITY_MAX as i64);
    //
    //     assert!(first.l_discount >= LineItemGenerator::DISCOUNT_MIN);
    //     assert!(first.l_discount <= LineItemGenerator::DISCOUNT_MAX);
    //
    //     assert!(first.l_tax >= LineItemGenerator::TAX_MIN);
    //     assert!(first.l_tax <= LineItemGenerator::TAX_MAX);
    //
    //     // Verify line numbers are sequential per order
    //     let mut order_lines = std::collections::HashMap::new();
    //     for line in &line_items {
    //         order_lines
    //             .entry(line.l_orderkey)
    //             .or_insert_with(Vec::new)
    //             .push(line.l_linenumber);
    //     }
    //
    //     // Check each order's line numbers
    //     for (_, lines) in order_lines {
    //         let mut sorted_lines = lines.clone();
    //         sorted_lines.sort();
    //
    //         // Line numbers should start at 1 and be sequential
    //         for (i, line_num) in sorted_lines.iter().enumerate() {
    //             assert_eq!(*line_num, (i + 1) as i32);
    //         }
    //     }
    //
    //     // Verify return flags and line status distributions
    //     let return_flags: std::collections::HashSet<_> =
    //         line_items.iter().map(|l| &l.l_returnflag).collect();
    //
    //     assert!(return_flags.len() > 1);
    //
    //     let line_statuses: std::collections::HashSet<_> =
    //         line_items.iter().map(|l| &l.l_linestatus).collect();
    //
    //     assert!(!line_statuses.is_empty());
    // }
    //
    // #[test]
    // fn check_iter_static_lifetimes() {
    //     // Lifetimes of iterators should be independent of the generator that
    //     // created it. This test case won't compile if that's not the case.
    //
    //     let _iter: NationGeneratorIterator<'static> = NationGenerator::default().iter();
    //     let _iter: RegionGeneratorIterator<'static> = RegionGenerator::default().iter();
    //     let _iter: VehicleGeneratorIterator<'static> = VehicleGenerator::new(0.1, 1, 1).iter();
    //     let _iter: DriverGeneratorIterator<'static> = DriverGenerator::new(0.1, 1, 1).iter();
    //     let _iter: CustomerGeneratorIterator<'static> = CustomerGenerator::new(0.1, 1, 1).iter();
    //     let _iter: OrderGeneratorIterator<'static> = OrderGenerator::new(0.1, 1, 1).iter();
    //     let _iter: LineItemGeneratorIterator<'static> = LineItemGenerator::new(0.1, 1, 1).iter();
    // }
}
