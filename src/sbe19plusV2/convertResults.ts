import { Table, FloatVector, predicate, Float32Vector, Column, Field } from 'apache-arrow';
import { pressure } from './equations/pressure';
import { temperature } from './equations/temperature';
import { conductivity } from './equations/conductivity';
import { salinity } from './equations/salinity';
import { oxygen } from './equations/oxygen';
import { depth} from './equations/depth';


export async function convertToEngineeringUnits (instrument: Object, coefficients: Object[], casts: Object,
                        df: Table) {
    /*
    Function to convert the raw, decimal data parsed from the hex file over to engineering units.
    The data has already been converted to decimal units from hexadecimal units in the df input.  This
    function relies heavily upon the equations found in the equations folder for the instrument-specific 
    conversion equations
    */
    let colName: string = "";
    let colName2: string = "";
    let msgArray = [];
    let sliceStart: number = 0, sliceEnd: number = 5;

    coefficients.forEach(x => {
        console.info(`coeff: ${JSON.stringify(x)}`);
    })

    console.info(`Elements ${sliceStart} to ${sliceEnd-1} of the columns:`)

    // Temperature (degC)
    colName = "Temperature A/D Counts";
    df = await temperature(df, colName, coefficients[0]['TemperatureSensor']);
    msgArray = df.getColumn('Temperature (degC)').toArray().slice(sliceStart, sliceEnd);
    console.info(`\tTemp: ${msgArray}`);

    // Pressure (dbars)
    colName = "Pressure A/D Counts";
    colName2 = "Pressure Temperature Compensation Voltage";
    df = await pressure(df, colName, colName2, coefficients[2]["PressureSensor"]);
    msgArray = df.getColumn('Pressure (dbars)').toArray().slice(sliceStart, sliceEnd);
    console.info(`\tPressure: ${msgArray}`);

    // Conductivity (S_per_m)
    colName = "Conductivity Frequency";
    df = await conductivity(df, colName, coefficients[1]["ConductivitySensor"]);
    msgArray = df.getColumn('Conductivity (S_per_m)').toArray().slice(sliceStart, sliceEnd);
    console.info(`\tConductivity: ${msgArray}`);

    // Salinity (psu)
    df = await salinity(df);
    msgArray = df.getColumn("Salinity (psu)").toArray().slice(sliceStart, sliceEnd);
    console.info(`\tSalinity: ${msgArray}`);

    // Oxygen, SBE 43 (ml_per_l)
    colName = "External Voltage 0";
    df = await oxygen(df, colName, coefficients[3]["OxygenSensor"]);
    msgArray = df.getColumn("Oxygen (ml_per_l)").toArray().slice(sliceStart, sliceEnd);
    console.info(`\tOxygen (SBE43): ${msgArray}`);

    console.info(`schema: ${df.schema.fields.map(x => x.name)}`);
    process.exit(0);

    // Fluorometer
    colName = "External Voltage 2";

    // Turbidity
    colName = "External Voltage 3";

    // Oxygen Optode, Aanderaa
    colName = "OPTODE Oxygen";

    console.info(`\nschema: ${df.schema.fields.map(x => x.name)}`);
    console.info(`item 0: ${df.get(0)}`);
    console.info(`item 1: ${df.get(1)}`);
    console.info(`item 2: ${df.get(2)}`);

    // Scan Rate - use for the temporal + spatial data integration
    let scanRate = 4;
}