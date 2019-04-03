import { readFileSync, close, writeFileSync, writeFile, createReadStream } from 'fs';
import * as path from 'path';
import {Table, Null} from 'apache-arrow';
import * as moment from 'moment';
import * as math from 'mathjs';
import * as parser from 'fast-xml-parser';
import { parseHex } from './src/sbe19plusV2/parseHex';
import { fetch } from 'node-fetch';
import * as axios from 'axios';

// Sample Data
const dir = "./data/sbe19plusV2/";
const hexFileName = "PORT_CTD5048_DO1360CT1460Op302_Hauls_1to5_21May2016.hex";
const xmlconFileName = "SBE19plusV2_5048.xmlcon";



async function bulkProcess() {

    // ToDo - Find all of the hex files and associated xmlcon files

    const hexFile = path.resolve(path.join(dir, hexFileName));
    const xmlconFile = path.resolve(path.join(dir, xmlconFileName));
    
    console.info(`hex file: ${hexFile}`);

    // Read an individiaul xmlcon file
    const xmlconFileInMemory = readFileSync(xmlconFile, "utf8");

    // Retrieve the xmlcon instrument and sensor details as JSON
    const xmlconJson = parser.parse(xmlconFileInMemory);
    const instrument = xmlconJson.SBE_InstrumentConfiguration.Instrument;
    const sensors = instrument.SensorArray.Sensor;

    // Parse hex file and convert to raw, decimal values in arrow data structure
    if (instrument.Name.indexOf("SBE 19plus V2") > -1) {

        // Retrieve Trawl Survey Haul Characteristics data from FRAM Data Warehouse
        let dwUrl = "https://www.nwfsc.noaa.gov/data/metadata/trawl.operation_haul_fact/selection.csv"
        const newFile = "C:\\Users\\Todd Hay\\Desktop\\hauls.arrow";
        const response = await axios.default(dwUrl);
        (dwUrl);
        const dataTable = await response.arrayBuffer().then((buffer: any) => {
            const newData = Table.from(new Uint8Array(buffer));
            writeFile(newFile, new Uint8Array(buffer), (err) => {
                if (err) throw err;
                console.log('The file has been saved!');
            });
            console.log(newData.get(0).toJSON());
            console.log(newData.get(0).toString());
            console.log('rows = ' + newData.count());
            return newData;
            return Table.from(new Uint8Array(buffer));
        });

        // Parse the SBE 19plusV2 hex file
        console.info('Parsing SBE19plusV2 file');
        await parseHex(hexFile, instrument, sensors);
    }

    // ToDo - Auto QA/QC the new arrow data structure

    // ToDo - Persist the data to disk

}

bulkProcess();