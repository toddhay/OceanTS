import { Table, FloatVector, predicate, Float32Vector, Column, Field } from 'apache-arrow';
import { temperature, pressure, conductivity } from '../equations';


export async function convertResults (instrument: Object, coefficients: Object[], casts: Object,
                        df: Table) {
    /*
    Function to convert the raw, decimal data parsed from the hex file over to engineering units.
    The data has already been converted to decimal units from hexadecimal units in the df input.  This
    function relies heavily upon the equations.ts file that contans all of the instrument-specific 
    conversion equations
    */
    let colName: string = "";
    let colName2: string = "";
    let msgArray = [];

    coefficients.forEach(x => {
        console.info(`coeff: ${JSON.stringify(x)}`);
    })

    // Temperature (degC)
    colName = "Temperature A/D Counts";
    df = await temperature(df, colName, coefficients[0]['TemperatureSensor']);
    msgArray = df.getColumn('Temperature (degC)').toArray().slice(-3);
    console.info(`temp: ${msgArray}`);

    // Pressure (dbars)
    colName = "Pressure A/D Counts";
    colName2 = "Pressure Temperature Compensation Voltage";
    df = await pressure(df, colName, colName2, coefficients[2]["PressureSensor"]);
    msgArray = df.getColumn('Pressure (dbars)').toArray().slice(-3);
    console.info(`pressure: ${msgArray}`);

    // Conductivity (S_per_m)
    colName = "Conductivity Frequency";
    df = await conductivity(df, colName, coefficients[1]["ConductivitySensor"]);
    msgArray = df.getColumn('Conductivity (S_per_m)').toArray().slice(-3);
    console.info(`conductivity: ${msgArray}`);

    // Oxygen, SBE 43 (ml_per_l)

    colName = "External Voltage 0";

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