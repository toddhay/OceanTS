import { Table, FloatVector, predicate, Float32Vector, Column, Field } from 'apache-arrow';
import { temperature } from '../equations';
import { col } from 'apache-arrow/compute/predicate';


export function convertResults (instrument: Object, coefficients: Object[], casts: Object,
                        df: Table) {
    /*
    Function to convert the raw, decimal data parsed from the hex file over to engineering units.
    The data has already been converted to decimal units from hexadecimal units in the df input.  This
    function relies heavily upon the equations.ts file that contans all of the instrument-specific 
    conversion equations
    */

    // Temperature (degC)
    let tColName = "Temperature A/D Counts";
    df = temperature(df, tColName, coefficients[0]['TemperatureSensor']);
    console.info(`temp: ${df.getColumn('Temperature (degC)').toArray().slice(-3)}`)

    // Conductivity


    // Pressure (dbars)


    // Oxygen

    
}