"""
This module contains functions that are used to generate the EPA eGRID database.

These functions are called in notebooks/EPA_eGRID.ipynb
"""


def determine_primary_fuel(bf_df, fuel_thresh, level='boiler'):
    """
    Determines the primary fuel of each boiler or each plant for each month and adds a new primary_fuel column.

    Args:
        bf_df (pd.DataFrame): a DataFrame of boiler_fuel_eia923 containing the following columns:
            - 'plant_id_eia'
            - 'boiler_id'
            - 'fuel_type_code'
            - 'report_date'
            - 'fuel_consumed_units'
            - 'fuel_mmbtu_per_unit'

        fuel_threshold = the threshold percentage above which a fuel is assigned as primary. For example, if set at 0.9, then if a single fuel makes up more than 90% of the heat input, it will be set as primary.

        level (string): specify whether you want boiler fuel proportion at plant level or boiler level. Default is boiler.

    Returns:
        pd.Dataframe: with columns ['plant_id_eia', ['boiler_id',] 'report_date', 'primary_fuel']

    """
    # Figure out the heat content proportions of each fuel received:
    pf_by_heat = calculate_fuel_percentage(bf_df, level=level)

    if level == 'boiler':
        # On a per boiler, per month basis, identify the fuel that made the largest
        # contribution to the plant's overall heat content consumed. If that
        # proportion is greater than fuel_thresh, set the primary_fuel to be
        # that fuel.  Otherwise, set it to "MULTI".
        pf_by_heat = pf_by_heat.set_index(
            ['plant_id_eia', 'boiler_id', 'report_date'])
    elif level == 'plant':
        # On a per plant, per month basis, identify the fuel that made the largest
        # contribution to the plant's overall heat content consumed. If that
        # proportion is greater than fuel_thresh, set the primary_fuel to be
        # that fuel.  Otherwise, set it to "MULTI".
        pf_by_heat = pf_by_heat.set_index(
            ['plant_id_eia', 'report_date'])

    # identify where percentage is greater than the threshold
    mask = pf_by_heat >= fuel_thresh
    pf_by_heat = pf_by_heat.where(mask)
    # create a new primary fuel column based on the name of the column with the primary fuel
    pf_by_heat['primary_fuel'] = pf_by_heat.idxmax(axis=1)
    pf_by_heat['primary_fuel'] = pf_by_heat['primary_fuel'].fillna(value='unknown')

    return pf_by_heat[['primary_fuel']].reset_index()


def calculate_fuel_percentage(bf_df, level='boiler'):
    """
    Calculates the percentage of heat input from each fuel type for each boiler for each month.

    Args:
        bf_df (pd.DataFrame): a DataFrame of boiler_fuel_eia923 containing the following columns:
            - 'plant_id_eia'
            - 'boiler_id'
            - 'fuel_type_code'
            - 'report_date'
            - 'fuel_consumed_units'
            - 'fuel_mmbtu_per_unit'
        level (string): specify whether you want boiler fuel proportion at plant level or boiler level. Default is boiler.
    """
    # calculate fuel consumption in mmbtu
    bf_df['fuel_consumed_mmbtu'] = bf_df['fuel_consumed_units'] * \
        bf_df['fuel_mmbtu_per_unit']

    if level == 'boiler':
        # drop fuel_consumed_units and fuel_mmbtu_per_unit columns
        bf_df = bf_df[['report_date',
                       'plant_id_eia',
                       'boiler_id',
                       'fuel_type_code',
                       'fuel_consumed_mmbtu']]

        # Take the individual rows organized by fuel_type_code, and turn them
        # into columns, each with the total MMBTU for that fuel, month, and boiler.
        fuel_pivot = bf_df.pivot_table(
            index=['report_date', 'plant_id_eia', 'boiler_id'],
            columns='fuel_type_code',
            values='fuel_consumed_mmbtu')

    elif level == 'plant':
        # drop fuel_consumed_units and fuel_mmbtu_per_unit columns
        bf_df = bf_df[['report_date',
                       'plant_id_eia',
                       'fuel_type_code',
                       'fuel_consumed_mmbtu']]

        # Group by report_date (monthly), plant_id_eia, and fuel_type
        bf_gb = bf_df.groupby(
            ['plant_id_eia', 'report_date', 'fuel_type_code']).sum().reset_index()

        # Take the individual rows organized by fuel_type_code, and turn them
        # into columns, each with the total MMBTU for that fuel, month, and boiler.
        fuel_pivot = bf_gb.pivot_table(
            index=['report_date', 'plant_id_eia'],
            columns='fuel_type_code',
            values='fuel_consumed_mmbtu')

    # Add a column that has the *total* heat content of all fuels:
    fuel_pivot['total'] = fuel_pivot.sum(axis=1, numeric_only=True)

    # Replace any NaN values we got from pivoting with zeros.
    fuel_pivot = fuel_pivot.fillna(value=0)

    # drop any months where zero fuel input was recorded
    fuel_pivot = fuel_pivot.drop(fuel_pivot[fuel_pivot['total'] == 0].index)

    # Divide all columns by the total heat content, giving us the proportions
    # for each fuel instead of the heat content.
    fuel_pivot = fuel_pivot.divide(fuel_pivot.total, axis='index')

    # Drop the total column (it's nothing but 1.0 values) and clean up the
    # index and columns a bit before returning the DF.
    fuel_pivot = fuel_pivot.drop('total', axis=1)
    fuel_pivot = fuel_pivot.reset_index()
    fuel_pivot.columns.name = None

    return fuel_pivot
