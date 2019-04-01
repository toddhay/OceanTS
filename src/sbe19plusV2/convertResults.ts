import { Table, FloatVector, predicate, Float32Vector, Column, Field } from 'apache-arrow';
import { temperature, pressure } from '../equations';
import { col } from 'apache-arrow/compute/predicate';


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
    msgArray = df.getColumn('Pressure (decibars)').toArray().slice(-3);
    console.info(`pressure: ${msgArray}`);

    // Conductivity
    colName = "Conductivity Frequency";

    // Oxygen, SBE 43

    colName = "External Voltage 0";

    // Fluorometer
    colName = "External Voltage 2";

    // Turbidity
    colName = "External Voltage 3";

    // Oxygen Optode, Aanderaa
    colName = "OPTODE Oxygen";

}