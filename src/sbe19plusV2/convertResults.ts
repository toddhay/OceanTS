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
import { logger, errorLogger } from '../logger';

export async function convertToEngineeringUnits (instrument: Object, coefficients: Object[], casts: Object[], 
                                                 voltageOffsets: Object, pumpDelay: number, df: Table,
                                                 outputFile: string, hauls?: Table, vessel?: string):
                                                 Promise<Table> {
    /*
    Function to convert the raw, decimal data parsed from the hex file over to engineering units.
    The data has already been converted to decimal units from hexadecimal units in the df input.  This
    function relies heavily upon the equations found in the equations folder for the instrument-specific 
    conversion equations
    */
    let msg: string = null;

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

    // Add Haul, Date/Time, Latitude, Longitude data to the arrow Table from the data warehouse
    logger.info(`\t\tMatching haul latitude/longitude data to casts in hex file`);
    casts = await mergeLatitudeIntoCasts(hauls, casts, vessel, scanRate);
    casts.forEach(x => {
        logger.info(`\t\t\tmatched cast: ${JSON.stringify(x)}`);
        if (!("latitude" in x)) {
            let outputArray = outputFile.split("\\");
            msg = `Year: ${moment(x["startDate"]).format("YYYY")}, ` +
                `Vessel: ${vessel}, Input File: ${outputArray[outputArray.length-1].replace(".csv", ".hex")}, ` +
                `Cast: ${x["cast"]} - could not fine matching haul`;
            logger.error(msg);
            errorLogger.error(msg);
            errorLogger.error(`\tcast: ${JSON.stringify(x)}`);
        }
    })

    // Depth - Requires Latitude data first
    df = await depth(df, casts);

    // Add haul ID, latitude, longitude, date/time into the arrow table
    logger.info(`\t\tAdd haul ID, latitude, longitude, and date/times into the arrow table`);
    try {
        df = await addHaulInfoToTable(df, casts);
    } catch (e) {
        let outputArray = outputFile.split("\\");
        msg = `Year: ${moment(casts[0]["startDate"]).format("YYYY")}, ` +
            `Vessel: ${vessel}, Input File: ${outputArray[outputArray.length-1].replace(".csv", ".hex")}, Error addHaulInfoToTable: ${e}`;
        logger.error(msg);
        errorLogger.error(msg);
    }

    // Return the arrow table
    return df;
}