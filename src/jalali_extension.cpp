#define DUCKDB_EXTENSION_MAIN

#include "jalali_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/types/date.hpp>
#include <duckdb/common/types/time.hpp>

namespace duckdb {

// Helper function to check leap year in Gregorian calendar
bool IsLeapYear(int year) {
    return (year % 400 == 0) || ((year % 100 != 0) && (year % 4 == 0));
}

// Helper function to convert Jalali date to Gregorian date with optional time component
duckdb::timestamp_t JalaliToGregorian(string_t jalali_datetime, bool end_of_day) {
    // Split the input into date and time components
    string datetime_str = jalali_datetime.GetString();
    auto datetime_parts = StringUtil::Split(datetime_str, ' '); // Split into date and time parts

    // Parse the date part
    auto date_part = datetime_parts[0];
    auto date_parts = StringUtil::Split(date_part, '-');
    if (date_parts.size() != 3) {
        throw InvalidInputException("Invalid Jalali date format. Expected format: YYYY-MM-DD");
    }
    int jy = stoi(date_parts[0]);
    int jm = stoi(date_parts[1]);
    int jd = stoi(date_parts[2]);

    jy -= 979;
    int j_day_no = 365 * jy + (jy / 33) * 8 + ((jy % 33 + 3) / 4);
    for (int i = 1; i < jm; ++i) {
        if (i <= 6) {
            j_day_no += 31;
        } else {
            j_day_no += 30;
        }
    }
    j_day_no += jd - 1;

    int g_day_no = j_day_no + 79;

    int gy = 1600 + 400 * (g_day_no / 146097);
    g_day_no %= 146097;
    bool leap = true;
    if (g_day_no >= 36525) {
        g_day_no--;
        gy += 100 * (g_day_no / 36524);
        g_day_no %= 36524;
        if (g_day_no >= 365) {
            g_day_no++;
        } else {
            leap = false;
        }
    }

    gy += 4 * (g_day_no / 1461);
    g_day_no %= 1461;
    if (g_day_no >= 366) {
        gy += (g_day_no - 1) / 365;
        g_day_no = (g_day_no - 1) % 365;
        leap = false;
    }

    int gm = 1;
    while (true) {
        int month_days;
        if (gm == 2) {
            month_days = leap ? 29 : 28;
        } else if (gm <= 7 && gm % 2 == 1) {
            month_days = 31;
        } else if (gm >= 8 && gm % 2 == 0) {
            month_days = 31;
        } else {
            month_days = 30;
        }

        if (g_day_no < month_days) {
            break;
        }
        g_day_no -= month_days;
        gm++;
    }
    int gd = g_day_no + 1;

    // Time component (default to 00:00:00 if not provided)
    int hour = 0, minute = 0, second = 0;
    if (datetime_parts.size() > 1) {
        // Time part exists, parse it
        auto time_part = datetime_parts[1];
        auto time_parts = StringUtil::Split(time_part, ':');
        if (time_parts.size() >= 2) {
            hour = stoi(time_parts[0]);
            minute = stoi(time_parts[1]);
            if (time_parts.size() == 3) {
                second = stoi(time_parts[2]);
            }
        }
    }

    // If `end_of_day` is true, set time to 23:59:59
    if (end_of_day) {
        hour = 23;
        minute = 59;
        second = 59;
    }

    // Create the `date_t` part
    auto gregorian_date = duckdb::Date::FromDate(gy, gm, gd);

    // Create the `dtime_t` part (time)
    auto gregorian_time = duckdb::Time::FromTime(hour, minute, second);

    // Return the combined timestamp
    return duckdb::Timestamp::FromDatetime(gregorian_date, gregorian_time);
}

// Scalar function for converting Jalali to Gregorian with time handling
inline void JalaliToGregorianScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &jalali_vector = args.data[0];
    auto &end_of_day_vector = args.data[1];

    // Using BinaryExecutor for two input parameters (string_t for date, bool for end_of_day)
    BinaryExecutor::Execute<string_t, bool, timestamp_t>(jalali_vector, end_of_day_vector, result, args.size(),
                                                   [&](string_t jalali_datetime, bool end_of_day) {
        // Convert Jalali datetime to Gregorian datetime
        return JalaliToGregorian(jalali_datetime, end_of_day);
    });
}



// Helper function to convert Gregorian date to Jalali date with optional time component
string GregorianToJalali(timestamp_t gregorian_timestamp) {
    // Extract date and time components
    date_t gregorian_date = Timestamp::GetDate(gregorian_timestamp);
    dtime_t gregorian_time = Timestamp::GetTime(gregorian_timestamp);

    int gy, gm, gd;
    Date::Convert(gregorian_date, gy, gm, gd);

    int jy, jm, jd;

    int g_days_in_month[] = { 31, (Date::IsLeapYear(gy) ? 29 : 28), 31, 30, 31, 30,
                              31, 31, 30, 31, 30, 31 };
    int j_days_in_month[] = { 31, 31, 31, 31, 31, 31,
                              30, 30, 30, 30, 30, 29 };

    int gy2 = gy - 1600;
    int gm2 = gm - 1;
    int gd2 = gd - 1;

    long g_day_no = 365 * gy2 + (gy2 + 3) / 4 - (gy2 + 99) / 100 + (gy2 + 399) / 400;
    for (int i = 0; i < gm2; ++i) {
        g_day_no += g_days_in_month[i];
    }
    g_day_no += gd2;

    long j_day_no = g_day_no - 79;

    long j_np = j_day_no / 12053;
    j_day_no = j_day_no % 12053;

    jy = 979 + 33 * j_np + 4 * (j_day_no / 1461);
    j_day_no %= 1461;

    if (j_day_no >= 366) {
        jy += (j_day_no - 1) / 365;
        j_day_no = (j_day_no - 1) % 365;
    }

    int i = 0;
    while (i < 11 && j_day_no >= j_days_in_month[i]) {
        j_day_no -= j_days_in_month[i];
        ++i;
    }
    jm = i + 1;
    jd = j_day_no + 1;

    // Construct time string
    int hour, minute, second, micros;
    Time::Convert(gregorian_time, hour, minute, second, micros);

    // Format the Jalali date and time
    char buffer[25];
    if (hour == 0 && minute == 0 && second == 0 && micros == 0) {
        // Date only
        snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d", jy, jm, jd);
    } else {
        // Date and time
        snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d %02d:%02d:%02d",
                 jy, jm, jd, hour, minute, second);
    }

    return string(buffer);
}

// Scalar function for converting Gregorian to Jalali with time handling
inline void GregorianToJalaliScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &gregorian_vector = args.data[0];

    UnaryExecutor::Execute<timestamp_t, string_t>(gregorian_vector, result, args.size(),
                                                  [&](timestamp_t gregorian_timestamp) {
        // Convert Gregorian timestamp to Jalali date string
        string jalali_datetime = GregorianToJalali(gregorian_timestamp);
        return StringVector::AddString(result, jalali_datetime);
    });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register the Jalali to Gregorian scalar function
    auto jalali_to_gregorian_scalar_function = ScalarFunction(
        "jalali_to_gregorian", {LogicalType::VARCHAR, LogicalType::BOOLEAN},
        LogicalType::TIMESTAMP, JalaliToGregorianScalarFun);
    ExtensionUtil::RegisterFunction(instance, jalali_to_gregorian_scalar_function);

    // Register the Gregorian to Jalali scalar function
    auto gregorian_to_jalali_scalar_function = ScalarFunction(
        "gregorian_to_jalali", {LogicalType::TIMESTAMP}, LogicalType::VARCHAR,
        GregorianToJalaliScalarFun);
    ExtensionUtil::RegisterFunction(instance, gregorian_to_jalali_scalar_function);
}

void JalaliExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
}

std::string JalaliExtension::Name() {
    return "jalali";
}

std::string JalaliExtension::Version() const {
#ifdef EXT_VERSION_JALALI
    return EXT_VERSION_JALALI;
#else
    return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void jalali_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::JalaliExtension>();
}

DUCKDB_EXTENSION_API const char *jalali_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
