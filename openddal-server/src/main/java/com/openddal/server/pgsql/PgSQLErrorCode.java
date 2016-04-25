/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.openddal.server.pgsql;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class PgSQLErrorCode {
    // Class 00 — Successful Completion

    public static final String successful_completion = "00000";// successful_completion
    // Class 01 — Warning
    public static final String warning = "01000";// warning
    public static final String dynamic_result_sets_returned = "0100C";// dynamic_result_sets_returned
    public static final String implicit_zero_bit_padding = "01008";// implicit_zero_bit_padding
    public static final String null_value_eliminated_in_set_function = "01003";// null_value_eliminated_in_set_function
    public static final String privilege_not_granted = "01007";// privilege_not_granted
    public static final String privilege_not_revoked = "01006";// privilege_not_revoked
    public static final String string_data_right_truncation_w = "01004";// string_data_right_truncation
    public static final String deprecated_feature = "01P01";// deprecated_feature
    // Class 02 — No Data (this is also a warning class per the SQL standard)
    public static final String no_data = "02000";// no_data
    public static final String no_additional_dynamic_result_sets_returned = "02001";// no_additional_dynamic_result_sets_returned
    // Class 03 — SQL Statement Not Yet Complete
    public static final String sql_statement_not_yet_complete = "03000";// sql_statement_not_yet_complete
    // Class 08 — Connection Exception
    public static final String connection_exception = "08000";// connection_exception
    public static final String connection_does_not_exist = "08003";// connection_does_not_exist
    public static final String connection_failure = "08006";// connection_failure
    public static final String sqlclient_unable_to_establish_sqlconnection = "08001";// sqlclient_unable_to_establish_sqlconnection
    public static final String sqlserver_rejected_establishment_of_sqlconnection = "08004";// sqlserver_rejected_establishment_of_sqlconnection
    public static final String transaction_resolution_unknown = "08007";// transaction_resolution_unknown
    public static final String protocol_violation = "08P01";// protocol_violation
    // Class 09 — Triggered Action Exception
    public static final String triggered_action_exception = "09000";// triggered_action_exception
    // Class 0A — Feature Not Supported
    public static final String feature_not_supported = "0A000";// feature_not_supported
    // Class 0B — Invalid Transaction Initiation
    public static final String invalid_transaction_initiation = "0B000";// invalid_transaction_initiation
    // Class 0F — Locator Exception
    public static final String locator_exception = "0F000";// locator_exception
    public static final String invalid_locator_specification = "0F001";// invalid_locator_specification
    // Class 0L — Invalid Grantor
    public static final String invalid_grantor = "0L000";// invalid_grantor
    public static final String invalid_grant_operation = "0LP01";// invalid_grant_operation
    // Class 0P — Invalid Role Specification
    public static final String invalid_role_specification = "0P000";// invalid_role_specification
    // Class 0Z — Diagnostics Exception
    public static final String diagnostics_exception = "0Z000";// diagnostics_exception
    public static final String stacked_diagnostics_accessed_without_active_handler = "0Z002";// stacked_diagnostics_accessed_without_active_handler
    // Class 20 — Case Not Found
    public static final String case_not_found = "20000";// case_not_found
    // Class 21 — Cardinality Violation
    public static final String cardinality_violation = "21000";// cardinality_violation
    // Class 22 — Data Exception
    public static final String data_exception = "22000";// data_exception
    public static final String array_subscript_error = "2202E";// array_subscript_error
    public static final String character_not_in_repertoire = "22021";// character_not_in_repertoire
    public static final String datetime_field_overflow = "22008";// datetime_field_overflow
    public static final String division_by_zero = "22012";// division_by_zero
    public static final String error_in_assignment = "22005";// error_in_assignment
    public static final String escape_character_conflict = "2200B";// escape_character_conflict
    public static final String indicator_overflow = "22022";// indicator_overflow
    public static final String interval_field_overflow = "22015";// interval_field_overflow
    public static final String invalid_argument_for_logarithm = "2201E";// invalid_argument_for_logarithm
    public static final String invalid_argument_for_ntile_function = "22014";// invalid_argument_for_ntile_function
    public static final String invalid_argument_for_nth_value_function = "22016";// invalid_argument_for_nth_value_function
    public static final String invalid_argument_for_power_function = "2201F";// invalid_argument_for_power_function
    public static final String invalid_argument_for_width_bucket_function = "2201G";// invalid_argument_for_width_bucket_function
    public static final String invalid_character_value_for_cast = "22018";// invalid_character_value_for_cast
    public static final String invalid_datetime_format = "22007";// invalid_datetime_format
    public static final String invalid_escape_character = "22019";// invalid_escape_character
    public static final String invalid_escape_octet = "2200D";// invalid_escape_octet
    public static final String invalid_escape_sequence = "22025";// invalid_escape_sequence
    public static final String nonstandard_use_of_escape_character = "22P06";// nonstandard_use_of_escape_character
    public static final String invalid_indicator_parameter_value = "22010";// invalid_indicator_parameter_value
    public static final String invalid_parameter_value = "22023";// invalid_parameter_value
    public static final String invalid_regular_expression = "2201B";// invalid_regular_expression
    public static final String invalid_row_count_in_limit_clause = "2201W";// invalid_row_count_in_limit_clause
    public static final String invalid_row_count_in_result_offset_clause = "2201X";// invalid_row_count_in_result_offset_clause
    public static final String invalid_tablesample_argument = "2202H";// invalid_tablesample_argument
    public static final String invalid_tablesample_repeat = "2202G";// invalid_tablesample_repeat
    public static final String invalid_time_zone_displacement_value = "22009";// invalid_time_zone_displacement_value
    public static final String invalid_use_of_escape_character = "2200C";// invalid_use_of_escape_character
    public static final String most_specific_type_mismatch = "2200G";// most_specific_type_mismatch
    public static final String null_value_not_allowed = "22004";// null_value_not_allowed
    public static final String null_value_no_indicator_parameter = "22002";// null_value_no_indicator_parameter
    public static final String numeric_value_out_of_range = "22003";// numeric_value_out_of_range
    public static final String string_data_length_mismatch = "22026";// string_data_length_mismatch
    public static final String string_data_right_truncation = "22001";// string_data_right_truncation
    public static final String substring_error = "22011";// substring_error
    public static final String trim_error = "22027";// trim_error
    public static final String unterminated_c_string = "22024";// unterminated_c_string
    public static final String zero_length_character_string = "2200F";// zero_length_character_string
    public static final String floating_point_exception = "22P01";// floating_point_exception
    public static final String invalid_text_representation = "22P02";// invalid_text_representation
    public static final String invalid_binary_representation = "22P03";// invalid_binary_representation
    public static final String bad_copy_file_format = "22P04";// bad_copy_file_format
    public static final String untranslatable_character = "22P05";// untranslatable_character
    public static final String not_an_xml_document = "2200L";// not_an_xml_document
    public static final String invalid_xml_document = "2200M";// invalid_xml_document
    public static final String invalid_xml_content = "2200N";// invalid_xml_content
    public static final String invalid_xml_comment = "2200S";// invalid_xml_comment
    public static final String invalid_xml_processing_instruction = "2200T";// invalid_xml_processing_instruction
    // Class 23 — Integrity Constraint Violation
    public static final String integrity_constraint_violation = "23000";// integrity_constraint_violation
    public static final String restrict_violation = "23001";// restrict_violation
    public static final String not_null_violation = "23502";// not_null_violation
    public static final String foreign_key_violation = "23503";// foreign_key_violation
    public static final String unique_violation = "23505";// unique_violation
    public static final String check_violation = "23514";// check_violation
    public static final String exclusion_violation = "23P01";// exclusion_violation
    // Class 24 — Invalid Cursor State
    public static final String invalid_cursor_state = "24000";// invalid_cursor_state
    // Class 25 — Invalid Transaction State
    public static final String invalid_transaction_state = "25000";// invalid_transaction_state
    public static final String active_sql_transaction = "25001";// active_sql_transaction
    public static final String branch_transaction_already_active = "25002";// branch_transaction_already_active
    public static final String held_cursor_requires_same_isolation_level = "25008";// held_cursor_requires_same_isolation_level
    public static final String inappropriate_access_mode_for_branch_transaction = "25003";// inappropriate_access_mode_for_branch_transaction
    public static final String inappropriate_isolation_level_for_branch_transaction = "25004";// inappropriate_isolation_level_for_branch_transaction
    public static final String no_active_sql_transaction_for_branch_transaction = "25005";// no_active_sql_transaction_for_branch_transaction
    public static final String read_only_sql_transaction = "25006";// read_only_sql_transaction
    public static final String schema_and_data_statement_mixing_not_supported = "25007";// schema_and_data_statement_mixing_not_supported
    public static final String no_active_sql_transaction = "25P01";// no_active_sql_transaction
    public static final String in_failed_sql_transaction = "25P02";// in_failed_sql_transaction
    // Class 26 — Invalid SQL Statement Name
    public static final String invalid_sql_statement_name = "26000";// invalid_sql_statement_name
    // Class 27 — Triggered Data Change Violation
    public static final String triggered_data_change_violation = "27000";// triggered_data_change_violation
    // Class 28 — Invalid Authorization Specification
    public static final String invalid_authorization_specification = "28000";// invalid_authorization_specification
    public static final String invalid_password = "28P01";// invalid_password
    // Class 2B — Dependent Privilege Descriptors Still Exist
    public static final String dependent_privilege_descriptors_still_exist = "2B000";// dependent_privilege_descriptors_still_exist
    public static final String dependent_objects_still_exist = "2BP01";// dependent_objects_still_exist
    // Class 2D — Invalid Transaction Termination
    public static final String invalid_transaction_termination = "2D000";// invalid_transaction_termination
    // Class 2F — SQL Routine Exception
    public static final String sql_routine_exception = "2F000";// sql_routine_exception
    public static final String function_executed_no_return_statement = "2F005";// function_executed_no_return_statement
    // public static final String modifying_sql_data_not_permitted ="2F002";//
    // modifying_sql_data_not_permitted
    // public static final String prohibited_sql_statement_attempted ="2F003";//
    // prohibited_sql_statement_attempted
    // public static final String reading_sql_data_not_permitted ="2F004";//
    // reading_sql_data_not_permitted
    // Class 34 — Invalid Cursor Name
    public static final String invalid_cursor_name = "34000";// invalid_cursor_name
    // Class 38 — External Routine Exception
    public static final String external_routine_exception = "38000";// external_routine_exception
    public static final String containing_sql_not_permitted = "38001";// containing_sql_not_permitted
    public static final String modifying_sql_data_not_permitted = "38002";// modifying_sql_data_not_permitted
    public static final String prohibited_sql_statement_attempted = "38003";// prohibited_sql_statement_attempted
    public static final String reading_sql_data_not_permitted = "38004";// reading_sql_data_not_permitted
    // Class 39 — External Routine Invocation Exception
    public static final String external_routine_invocation_exception = "39000";// external_routine_invocation_exception
    public static final String invalid_sqlstate_returned = "39001";// invalid_sqlstate_returned
    // public static final String null_value_not_allowed ="39004";//
    // null_value_not_allowed
    public static final String trigger_protocol_violated = "39P01";// trigger_protocol_violated
    public static final String srf_protocol_violated = "39P02";// srf_protocol_violated
    public static final String event_trigger_protocol_violated = "39P03";// event_trigger_protocol_violated
    // Class 3B — Savepoint Exception
    public static final String savepoint_exception = "3B000";// savepoint_exception
    public static final String invalid_savepoint_specification = "3B001";// invalid_savepoint_specification
    // Class 3D — Invalid Catalog Name
    public static final String invalid_catalog_name = "3D000";// invalid_catalog_name
    // Class 3F — Invalid Schema Name
    public static final String invalid_schema_name = "3F000";// invalid_schema_name
    // Class 40 — Transaction Rollback
    public static final String transaction_rollback = "40000";// transaction_rollback
    public static final String transaction_integrity_constraint_violation = "40002";// transaction_integrity_constraint_violation
    public static final String serialization_failure = "40001";// serialization_failure
    public static final String statement_completion_unknown = "40003";// statement_completion_unknown
    public static final String deadlock_detected = "40P01";// deadlock_detected
    // Class 42 — Syntax Error or Access Rule Violation
    public static final String syntax_error_or_access_rule_violation = "42000";// syntax_error_or_access_rule_violation
    public static final String syntax_error = "42601";// syntax_error
    public static final String insufficient_privilege = "42501";// insufficient_privilege
    public static final String cannot_coerce = "42846";// cannot_coerce
    public static final String grouping_error = "42803";// grouping_error
    public static final String windowing_error = "42P20";// windowing_error
    public static final String invalid_recursion = "42P19";// invalid_recursion
    public static final String invalid_foreign_key = "42830";// invalid_foreign_key
    public static final String invalid_name = "42602";// invalid_name
    public static final String name_too_long = "42622";// name_too_long
    public static final String reserved_name = "42939";// reserved_name
    public static final String datatype_mismatch = "42804";// datatype_mismatch
    public static final String indeterminate_datatype = "42P18";// indeterminate_datatype
    public static final String collation_mismatch = "42P21";// collation_mismatch
    public static final String indeterminate_collation = "42P22";// indeterminate_collation
    public static final String wrong_object_type = "42809";// wrong_object_type
    public static final String undefined_column = "42703";// undefined_column
    public static final String undefined_function = "42883";// undefined_function
    public static final String undefined_table = "42P01";// undefined_table
    public static final String undefined_parameter = "42P02";// undefined_parameter
    public static final String undefined_object = "42704";// undefined_object
    public static final String duplicate_column = "42701";// duplicate_column
    public static final String duplicate_cursor = "42P03";// duplicate_cursor
    public static final String duplicate_database = "42P04";// duplicate_database
    public static final String duplicate_function = "42723";// duplicate_function
    public static final String duplicate_prepared_statement = "42P05";// duplicate_prepared_statement
    public static final String duplicate_schema = "42P06";// duplicate_schema
    public static final String duplicate_table = "42P07";// duplicate_table
    public static final String duplicate_alias = "42712";// duplicate_alias
    public static final String duplicate_object = "42710";// duplicate_object
    public static final String ambiguous_column = "42702";// ambiguous_column
    public static final String ambiguous_function = "42725";// ambiguous_function
    public static final String ambiguous_parameter = "42P08";// ambiguous_parameter
    public static final String ambiguous_alias = "42P09";// ambiguous_alias
    public static final String invalid_column_reference = "42P10";// invalid_column_reference
    public static final String invalid_column_definition = "42611";// invalid_column_definition
    public static final String invalid_cursor_definition = "42P11";// invalid_cursor_definition
    public static final String invalid_database_definition = "42P12";// invalid_database_definition
    public static final String invalid_function_definition = "42P13";// invalid_function_definition
    public static final String invalid_prepared_statement_definition = "42P14";// invalid_prepared_statement_definition
    public static final String invalid_schema_definition = "42P15";// invalid_schema_definition
    public static final String invalid_table_definition = "42P16";// invalid_table_definition
    public static final String invalid_object_definition = "42P17";// invalid_object_definition
    // Class 44 — WITH CHECK OPTION Violation
    public static final String with_check_option_violation = "44000";// with_check_option_violation
    // Class 53 — Insufficient Resources
    public static final String insufficient_resources = "53000";// insufficient_resources
    public static final String disk_full = "53100";// disk_full
    public static final String out_of_memory = "53200";// out_of_memory
    public static final String too_many_connections = "53300";// too_many_connections
    public static final String configuration_limit_exceeded = "53400";// configuration_limit_exceeded
    // Class 54 — Program Limit Exceeded
    public static final String program_limit_exceeded = "54000";// program_limit_exceeded
    public static final String statement_too_complex = "54001";// statement_too_complex
    public static final String too_many_columns = "54011";// too_many_columns
    public static final String too_many_arguments = "54023";// too_many_arguments
    // Class 55 — Object Not In Prerequisite State
    public static final String object_not_in_prerequisite_state = "55000";// object_not_in_prerequisite_state
    public static final String object_in_use = "55006";// object_in_use
    public static final String cant_change_runtime_param = "55P02";// cant_change_runtime_param
    public static final String lock_not_available = "55P03";// lock_not_available
    // Class 57 — Operator Intervention
    public static final String operator_intervention = "57000";// operator_intervention
    public static final String query_canceled = "57014";// query_canceled
    public static final String admin_shutdown = "57P01";// admin_shutdown
    public static final String crash_shutdown = "57P02";// crash_shutdown
    public static final String cannot_connect_now = "57P03";// cannot_connect_now
    public static final String database_dropped = "57P04";// database_dropped
    // Class 58 — System Error (errors external to PostgreSQL itself)
    public static final String system_error = "58000";// system_error
    public static final String io_error = "58030";// io_error
    public static final String undefined_file = "58P01";// undefined_file
    public static final String duplicate_file = "58P02";// duplicate_file
    // Class F0 — Configuration File Error
    public static final String config_file_error = "F0000";// config_file_error
    public static final String lock_file_exists = "F0001";// lock_file_exists
    // Class HV — Foreign Data Wrapper Error (SQL/MED)
    public static final String fdw_error = "HV000";// fdw_error
    public static final String fdw_column_name_not_found = "HV005";// fdw_column_name_not_found
    public static final String fdw_dynamic_parameter_value_needed = "HV002";// fdw_dynamic_parameter_value_needed
    public static final String fdw_function_sequence_error = "HV010";// fdw_function_sequence_error
    public static final String fdw_inconsistent_descriptor_information = "HV021";// fdw_inconsistent_descriptor_information
    public static final String fdw_invalid_attribute_value = "HV024";// fdw_invalid_attribute_value
    public static final String fdw_invalid_column_name = "HV007";// fdw_invalid_column_name
    public static final String fdw_invalid_column_number = "HV008";// fdw_invalid_column_number
    public static final String fdw_invalid_data_type = "HV004";// fdw_invalid_data_type
    public static final String fdw_invalid_data_type_descriptors = "HV006";// fdw_invalid_data_type_descriptors
    public static final String fdw_invalid_descriptor_field_identifier = "HV091";// fdw_invalid_descriptor_field_identifier
    public static final String fdw_invalid_handle = "HV00B";// fdw_invalid_handle
    public static final String fdw_invalid_option_index = "HV00C";// fdw_invalid_option_index
    public static final String fdw_invalid_option_name = "HV00D";// fdw_invalid_option_name
    public static final String fdw_invalid_string_length_or_buffer_length = "HV090";// fdw_invalid_string_length_or_buffer_length
    public static final String fdw_invalid_string_format = "HV00A";// fdw_invalid_string_format
    public static final String fdw_invalid_use_of_null_pointer = "HV009";// fdw_invalid_use_of_null_pointer
    public static final String fdw_too_many_handles = "HV014";// fdw_too_many_handles
    public static final String fdw_out_of_memory = "HV001";// fdw_out_of_memory
    public static final String fdw_no_schemas = "HV00P";// fdw_no_schemas
    public static final String fdw_option_name_not_found = "HV00J";// fdw_option_name_not_found
    public static final String fdw_reply_handle = "HV00K";// fdw_reply_handle
    public static final String fdw_schema_not_found = "HV00Q";// fdw_schema_not_found
    public static final String fdw_table_not_found = "HV00R";// fdw_table_not_found
    public static final String fdw_unable_to_create_execution = "HV00L";// fdw_unable_to_create_execution
    public static final String fdw_unable_to_create_reply = "HV00M";// fdw_unable_to_create_reply
    public static final String fdw_unable_to_establish_connection = "HV00N";// fdw_unable_to_establish_connection
    // Class P0 — PL/pgSQL Error
    public static final String plpgsql_error = "P0000";// plpgsql_error
    public static final String raise_exception = "P0001";// raise_exception
    public static final String no_data_found = "P0002";// no_data_found
    public static final String too_many_rows = "P0003";// too_many_rows
    public static final String assert_failure = "P0004";// assert_failure
    // Class XX — Internal Error
    public static final String internal_error = "XX000";// internal_error
    public static final String data_corrupted = "XX001";// data_corrupted
    public static final String index_corrupted = "XX002";// index_corrupted
}
