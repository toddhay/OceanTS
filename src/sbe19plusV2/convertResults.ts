import { Table, FloatVector, predicate, Float32Vector, Column, Field } from 'apache-arrow';
import { pressure } from './equations/pressure';
import { temperature } from './equations/temperature';
import { conductivity } from './equations/conductivity';
import { salinity } from './equations/salinity';
import { oxygen_sbe43, oxygen_optode } from './equations/oxygen';
import { depth} from './equations/depth';
import { turbidity } from './equations/turbidity';
import { fluorescence } from './equations/fluorescence';


export async function convertToEngineeringUnits (instrument: Object,
                                                 coefficients: Object[], 
                                                 casts: Object, 
                                                 voltageOffsets: Object,
                                                 pumpDelay: number,
                                                 df: Table) {
    /*
    Function to convert the raw, decimal data parsed from the hex file over to engineering units.
    The data has already been converted to decimal units from hexadecimal units in the df input.  This
    function relies heavily upon the equations found in the equations folder for the instrument-specific 
    conversion equations
    */
   
    // Scan Rate - use for the temporal + spatial data integration
    let scanRate = 4;

    // Temperature (degC)
    df = await temperature(df, "Temperature A/D Counts", coefficients[0]['TemperatureSensor']);

    // Pressure (dbars)
    df = await pressure(df, "Pressure A/D Counts", "Pressure Temperature Compensation Voltage", coefficients[2]["PressureSensor"]);

    // Conductivity (S_per_m)
    df = await conductivity(df, "Conductivity Frequency", coefficients[1]["ConductivitySensor"]);

    // Salinity (psu)
    df = await salinity(df);

    // Oxygen, SBE 43 (ml_per_l)

    // ToDo Add Voltage 0 offset/slope
    df = await oxygen_sbe43(df, "External Voltage 0", coefficients[3]["OxygenSensor"], scanRate);

    // Fluorometer - "External Voltage 2"
    // ToDo Add Voltage 0 offset/slope
    
    // Turbidity - "External Voltage 3"
    // ToDo Add Voltage 0 offset/slope
    
    // Oxygen Optode, Aanderaa
    df = await oxygen_optode(df, "OPTODE Oxygen", coefficients[6]["OptodeOxygenAanderaa"]);

    // Split into separate hauls based on the casts

    // Retrieve Haul, Date/Time, Latitude, Longitude data from the FRAM Data Warehouse
    
    // Depth - Requires Latitude data first


    // Display the results
    let msgArray = ["Temperature (degC)", "Pressure (dbars)", "Conductivity (S_per_m)",
        "Salinity (psu)", "Oxygen (ml_per_l)", "OPTODE Oxygen (ml_per_l)"];
    let results = [];
    let sliceStart: number = 30, sliceEnd: number = 35;

    console.info("Calibration Coefficients");
    coefficients.forEach(x => { console.info(`\tcoeff: ${JSON.stringify(x)}`); });
    console.info(`Elements ${sliceStart} to ${sliceEnd-1} of the columns:`)
    msgArray.forEach(x => {
        results = df.getColumn(x).toArray().slice(sliceStart, sliceEnd);
        console.info(`\t${x}: ${results}`);
    });
    console.info(`Schema: ${df.schema.fields.map(x => x.name)}`);
    console.info(`Voltage Offsets: ${JSON.stringify(voltageOffsets)}`);
}