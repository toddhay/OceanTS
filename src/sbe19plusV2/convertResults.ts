import { Table } from 'apache-arrow';
import { pressure } from './equations/pressure';
import { temperature } from './equations/temperature';
import { conductivity } from './equations/conductivity';
import { salinity } from './equations/salinity';
import { oxygen_sbe43, oxygen_optode } from './equations/oxygen';
import { depth} from './equations/depth';
import { turbidity } from './equations/turbidity';
import { fluorescence } from './equations/fluorescence';
import * as moment from 'moment';
import * as os from 'os';
import * as path from 'path';
import { mergeLatitudeIntoCasts, addHaulInfoToTable, saveToFile } from '../utilities';


export async function convertToEngineeringUnits (instrument: Object, coefficients: Object[], casts: Object[], 
                                                 voltageOffsets: Object, pumpDelay: number, df: Table,
                                                 hauls?: Table, vessel?: string) {
    /*
    Function to convert the raw, decimal data parsed from the hex file over to engineering units.
    The data has already been converted to decimal units from hexadecimal units in the df input.  This
    function relies heavily upon the equations found in the equations folder for the instrument-specific 
    conversion equations
    */
    console.info(`Converting to Engineering Units`);
    let start = moment();

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

    let end = moment();
    let duration = moment.duration(end.diff(start)).asSeconds();
    console.info(`\tProcessing time - converting to engineering units: ${duration}s`);

    // Add Haul, Date/Time, Latitude, Longitude data to the arrow Table from the data warehouse
    console.info(`Matching haul latitude/longitude data to cast`);
    start = moment();
    casts = await mergeLatitudeIntoCasts(hauls, casts, vessel, scanRate);
    end = moment();
    duration = moment.duration(end.diff(start)).asSeconds();
    casts.forEach(x => {
        console.info(`\t${JSON.stringify(x)}`);
    })
    console.info(`\tProcessing time - matching haul lat/lons to casts: ${duration}s`);

    // Depth - Requires Latitude data first
    console.info(`Calculating Depth (m)`);
    start = moment();
    df = await depth(df, casts);
    end = moment();
    duration = moment.duration(end.diff(start)).asSeconds();
    console.info(`\tProcessing time - calculating depth (m): ${duration}s`);

    // Add haul ID, latitude, longitude, date/time into the arrow table
    console.info(`Add haul ID, latitude, longitude, and date/times into the arrow table`);
    start = moment();
    df = await addHaulInfoToTable(df, casts);
    end = moment();
    duration = moment.duration(end.diff(start)).asSeconds();
    console.info(`\tProcessing time - adding haul info to table: ${duration}s`);

    // Save the results to a csv file
    console.info(`Saving data to a csv file`);
    start = moment();
    let filename = path.join(os.homedir(), "Desktop", "test.csv");
    let outputColumns = ["Temperature (degC)", "Pressure (dbars)", "Conductivity (S_per_m)",
        "Salinity (psu)", "Oxygen (ml_per_l)", "OPTODE Oxygen (ml_per_l)", "Depth (m)",
        "Latitude (decDeg)", "Longitude (decDeg)", "HaulID", "DateTime (ISO8601)", "Year", "Month", "Day"
    ];
    await saveToFile(df, "csv", filename, outputColumns);
    end = moment();
    duration = moment.duration(end.diff(start)).asSeconds();
    console.info(`\tProcessing time - saving result to a file: ${duration}s`);

    // Display the results
    let results = [];

    let sliceSize: number = 5, 
        sliceStart: number = 0, //df.length - sliceSize, 
        sliceEnd: number = sliceStart + sliceSize;

    // console.info("Calibration Coefficients");
    // coefficients.forEach(x => { console.info(`\tcoeff: ${JSON.stringify(x)}`); });
    console.info(`Results - Elements ${sliceStart} to ${sliceEnd-1} of the columns:`)
    outputColumns.forEach(x => {
        results = df.getColumn(x).toArray().slice(sliceStart, sliceEnd);
        console.info(`\t${x}: ${results}`);
    });
    // console.info(`Schema: ${df.schema.fields.map(x => x.name)}`);
    // console.info(`Voltage Offsets: ${JSON.stringify(voltageOffsets)}`);
    // console.info(`Casts: ${JSON.stringify(casts)}`);
}