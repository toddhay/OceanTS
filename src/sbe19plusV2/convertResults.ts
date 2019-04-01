import { Table, FloatVector, predicate, Float32Vector, Column, Field } from 'apache-arrow';
import { temperature, pressure } from '../equations';
import { col } from 'apache-arrow/compute/predicate';


export function convertResults (instrument: Object, coefficients: Object[], casts: Object,
                        df: Table) {
    /*
    Function to convert the raw, decimal data parsed from the hex file over to engineering units.
    The data has already been converted to decimal units from hexadecimal units in the df input.  This
    function relies heavily upon the equations.ts file that contans all of the instrument-specific 
    conversion equations
    */
    let colName: string = "";
    let colName2: string = "";

    coefficients.forEach(x => {
        console.info(`coeff: ${JSON.stringify(x)}`);
    })

    // Temperature (degC)
    colName = "Temperature A/D Counts";
    df = temperature(df, colName, coefficients[0]['TemperatureSensor']);
    console.info(`temp: ${df.getColumn('Temperature (degC)').toArray().slice(-3)}`);

    // Pressure (dbars)
    colName = "Pressure A/D Counts";
    colName2 = "Pressure Temperature Compensation Voltage";
    df = pressure(df, colName, colName2, coefficients[1]["ConductivitySensor"]);

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