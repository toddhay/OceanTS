import { Table, DateVector, Float32Vector, Utf8Vector } from "apache-arrow";
import Axios from 'axios';
import * as os from 'os';
import * as path from 'path';
import { readFileSync, existsSync, writeFileSync, readdirSync } from 'fs';
import { readFile } from "xlsx/types";
import * as csv from 'csvtojson';
import * as moment from 'moment';
import * as glob from 'glob';
import * as fg from 'fast-glob';

export function hex2dec(x: string): number {
    return parseInt(x, 16);
}

export function counts2frequency(counts: number): number {
    /* Function to convert Analog/Digital counts to a frequency
        This is used for temperature, pressure, and voltage A/D counts

        counts:  number with 6 digits, each 2 digit group represents a byte of data

        return:  frequency - number
    */

    if (counts.toString().length !== 6) {
        console.error('Counts number of digits is not 6: ' + counts);
        return NaN;
    }
    let countsStr = counts.toString();
    return parseInt(countsStr.slice(0,2)) * 256 +
            parseInt(countsStr.slice(2,4)) +
            parseInt(countsStr.slice(4,6)) / 256

}

export function jsonArray2ArrowTable(jsonData: Object[]): Table {
    let header = Object.keys(jsonData[0]);
    let dataArrays = [];
    let temp: any = null;
    header.forEach(h => {
        temp = jsonData.map(x => x[h]);
        if (['latitude_hi_prec_dd', 'longitude_hi_prec_dd'].includes(h)) {
            dataArrays.push(Float32Vector.from(temp.map((x: any) => parseFloat(x))));
        } else if (['tow_end_timestamp', 'tow_start_timestamp'].includes(h)) { 
            dataArrays.push(DateVector.from(temp.map((x: any) => moment(x, "YYYY-MM-DD HH:mm:ss").toDate())));
        } else {
            dataArrays.push(Utf8Vector.from(temp));
        } 
    })
    let df: Table = Table.new(dataArrays, header);
    return df;
}

export async function getTrawlSurveyHaulData(): Promise<Table> {
    /*


    Useful Arrow JS page on json2arrow:
    https://github.com/apache/arrow/blob/master/js/bin/json-to-arrow.js
    */

    // Retrieve Trawl Survey Haul Characteristics data from FRAM Data Warehouse
    let baseUrl = "https://www.nwfsc.noaa.gov/data/api/v1/source/trawl.operation_haul_fact/selection.";
    let selectionType = "csv";  // "json"
    let variables = "latitude_hi_prec_dd,longitude_hi_prec_dd,tow_end_timestamp,tow_start_timestamp,trawl_id,vessel";
    let filters = "year>=2016,year<=2018";
    let dwUrl = baseUrl + selectionType + "?" + "filters=" + filters + "&" + "variables=" + variables;
    console.info(`dwUrl = ${dwUrl}`);

    const desktopDir = path.join(os.homedir(), "Desktop");
    const haulsArrowFile = path.join(desktopDir, "hauls.arrow");
    const haulsFile = path.join(desktopDir, "hauls." + selectionType);
    try {
        let data: any;
        if (!existsSync(haulsFile)) {
            const response = await Axios.get(dwUrl);
            data = response.data;
            if (selectionType === "json") {
                writeFileSync(haulsFile, JSON.stringify(data));
            } else {
                writeFileSync(haulsFile, data);
                data = await csv().fromString(data);    // Convert csv to an array of JSON objects
            }
            console.info(`data retrieved successfully`);
        } else {
            if (selectionType === "csv") {
                data = await csv().fromFile(haulsFile);   // Convert csv to an array of JSON objects
            } else {
                data = readFileSync(haulsFile);
            }
        }
        console.info(`haul data successfully opened ...`);

        let df = jsonArray2ArrowTable(data);
        console.info(`haul data successfully converted to an arrow table`);

        return df;
    } catch (e) {
        console.error(`Error in retrieving trawl survey haul data: ${e}`);
    }
    return null;
}

export async function getHexFiles(dataDir: string): Promise<Array<string>> {
    let hexFiles: string[] = [];
    hexFiles = fg.sync([dataDir + '/**/*.hex'], 
        {nocase: true, ignore: [
            dataDir + '/**/*test*.hex',
            dataDir + '/**/*calibration*.hex',
            dataDir + '/**/OrangeBoatCastTemplates/**/*.hex',
            dataDir + '/**/BlueBoatCastTemplates/**/*.hex',
    ]});
    return hexFiles;
}