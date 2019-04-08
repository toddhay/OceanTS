import { Table, Vector, RecordBatchWriter, RecordBatchStreamReader, RecordBatchFileReader, RecordBatchReader  } from "apache-arrow";
// import { JSONDataLoader } from 'apache-arrow/ipc/reader';
import Axios from 'axios';
import * as os from 'os';
import * as path from 'path';
import { createWriteStream, readFileSync } from 'fs';

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

export async function getTrawlSurveyHaulData(): Promise<Table> {
    /*


    Useful Arrow JS page on json2arrow:
    https://github.com/apache/arrow/blob/master/js/bin/json-to-arrow.js
    */


    const desktopDir = path.join(os.homedir(), "Desktop");
    const haulsArrowFile = path.join(desktopDir, "hauls.arrow");
    const haulsJSONFile = path.join(desktopDir, "hauls.json");

    // Retrieve Trawl Survey Haul Characteristics data from FRAM Data Warehouse
    let filters = "tow_end_timestamp,tow_start_timestamp,vessel,trawl_id,latitude_hi_prec_dd,longitude_hi_prec_dd";
    let selectionType = "csv";  // "json"
    let baseUrl = "https://www.nwfsc.noaa.gov/data/api/v1/source/trawl.operation_haul_fact/selection.";
    let dwUrl = baseUrl + selectionType + "?variables=" + filters;

    let TEST: boolean = true;
    if (TEST) dwUrl += "&year=2018";

    console.info(`dwUrl = ${dwUrl}`);
    process.exit(0);
    try {
        // WORKS for pulling down the haul data in either json or csv format
        // const response = await Axios.get(dwUrl);
        // const data = response.data;

        const reader = RecordBatchFileReader.from(readFileSync(haulsJSONFile));
        console.info(`reader: ${JSON.stringify(reader)}`);
        const data = Table.from(reader);
        return data;
        // const jsonToArrow = await reader.pipe(RecordBatchWriter.throughNode())
        //     .pipe(createWriteStream(haulsArrowFile));


        // const response = await Axios.get(dwUrl);
        // const reader = await RecordBatchReader.from(JSON.parse(response.data))
        // const jsonToArrow = await reader.pipe(RecordBatchWriter.throughNode())
        //     .pipe(createWriteStream(newFile));

        // const response = await Axios.get(dwUrl, {
        //     responseType: 'arraybuffer',
        //     headers: {'Accept': 'text/csv' }
        // }).then(response => {
        //     const data = Table.from(new Uint8Array(response.data));
        //     return data;
        // });
    } catch (e) {
        console.error(`Error in retrieving trawl survey haul data: ${e}`);
    }
    return null;
}