import { readFileSync, writeFileSync, writeFile, createReadStream } from 'fs';
import { createInterface } from 'readline';
import {Table, Null} from 'apache-arrow';
import fetch from 'node-fetch';
import * as moment from 'moment';

const line_counter = ((i = 0) => () => ++i)();

const file = "./data/sbe19plusV2/2016_Excalibur/Excalibur_2016_CTD_Leg1/SeabirdOps/PORT_CTD5048_DO1360CT1460Op302_Hauls_1to5_21May2016.hex";

const lineReader = createInterface({
    input: createReadStream(file)
});

let dataStartLine: number = -1;
let serialNumber: any = null;
let startDateTime: any = null;
let parts: any = null;
let subparts: any = null;
let sensors = {};
let extraSensors = {};
let voltages = {};
let currentChar = -1;
let t: number, c: number, p: number, ptcv: number = null, oxygen: number,
    oxygenPhase: number, oxygenTempVoltage: number, gtdPressure: number, gtdTemp: number;
let voltage0: number, voltage1:number, voltage2: number, 
    voltage3: number, voltage4:number, voltage5: number;

const parsing = [
    {"variable": "temperature", "size": 6, "operations": null},
    {"variable": "conductivity", "size": 6, "operations": null}
]
let msg: string = null;

lineReader.on('line', (line, lineno = line_counter()) => {

    if ((lineno > dataStartLine+2) && (dataStartLine !== -1)) lineReader.close();

    if ((line.startsWith('* SBE 19plus V 2.5.2')) && (startDateTime === null)) {
        const lineParts = line.split("SERIAL NO.").map(s => s.trim());
        if (lineParts.length === 2) {
            serialNumber = lineParts[1].split(' ')[0];
            startDateTime = lineParts[1].replace(serialNumber, "").trim()
            console.info('startDateTime: ' + startDateTime);
        }
        // console.info('parts: ' + lineParts);
        // startDateTime = line.slice(-20).trim();
    }

    if (line.startsWith("* pressure sensor")) {
        parts = line.split(",").map(s => s.replace("*", "").trim());
        parts.forEach((x: any) => {
            subparts = x.split("=").map(s => s.trim());
            if (subparts.length === 2) 
                sensors[subparts[0]] = subparts[1];
        })
    }
    if (line.startsWith("* SBE 38")) {
        // Parse for determining if extra sensors exist or not
        const sensorParts = line.split(",");
        sensorParts.forEach(x => {
            parts = x.split("=").map(s => s.replace("*", "").trim());
            if (parts.length === 2) {
                extraSensors[parts[0]] = parts[1] === "yes" ? true : false;
            }
        })
    }

    if (line.startsWith("* Ext Volt")) {
        parts = line.split(",").map(s => s.replace("*", "").trim());
        parts.forEach((x: any) => {
            subparts = x.split("=").map(s => s.trim());
            if (subparts.length === 2) 
                voltages[subparts[0]] = subparts[1] === "yes" ? true : false;
        })
    }

    if (line.startsWith('*END*')) { 
        console.info('sensors: ' + JSON.stringify(sensors));
        console.info('extraSensors: ' + JSON.stringify(extraSensors));
        console.info('voltages: '+ JSON.stringify(voltages));

        dataStartLine = lineno + 1; 
        console.info('dataStartLine: ' + dataStartLine);
    }

    if ((lineno >= dataStartLine) && (dataStartLine !== -1)) {
        // Parse Data
        msg = '';

        // Fixed Sized Data, no need to dynamically check if voltages/sensors exist
        t = parseInt(line.slice(0,6), 16);
        c = parseInt(line.slice(6,12), 16) / 256;
        if (sensors["pressure sensor"] === "strain gauge") {
            p = parseInt(line.slice(12,18), 16);
            ptcv = parseInt(line.slice(18,22), 16) / 13107;    
        } else if (sensors["pressure sensor"] === "quartz pressure") {
            p = parseInt(line.slice(12,18), 16) / 256;
            ptcv = parseInt(line.slice(18,22), 16) / 13107;    
        }
        msg += t + ', ' + c + ', ' + p + ', ' + ptcv;
        currentChar = 22;

        // Dynamically Sized Data, need to check if the voltages/sensors exist
        // Todo - Fix so we don't hardcode in the character places to parse
        if (voltages["Ext Volt 0"]) { 
            voltage0 = parseInt(line.slice(currentChar, currentChar+4), 16) / 13107;
            console.info('Volt 0', line.slice(currentChar, currentChar+4), 
                parseInt(line.slice(currentChar, currentChar+4), 16), voltage0);
            msg += ' Volt 0=' + voltage0
            currentChar += 4;
        }
        if (voltages["Ext Volt 1"]) {
            voltage1 = parseInt(line.slice(currentChar, currentChar+4), 16) / 13107;
            msg += ' Volt 1=' + voltage1
            currentChar +=4;
        }
        if (voltages["Ext Volt 2"]) {
            voltage2 = parseInt(line.slice(currentChar, currentChar+4), 16) / 13107;
            console.info('Volt 2', line.slice(currentChar, currentChar+4),
                parseInt(line.slice(currentChar, currentChar+4), 16), voltage2);
            msg += ' Volt 2=' + voltage2
            currentChar +=4;
        }
        
        if (voltages["Ext Volt 3"]) {
            voltage3 = parseInt(line.slice(currentChar, currentChar+4), 16) / 13107;
            console.info('Volt 3', line.slice(currentChar, currentChar+4), 
                parseInt(line.slice(currentChar, currentChar+4), 16), voltage3);
            msg += ' Volt 3=' + voltage3
            currentChar +=4;
        }
        if (voltages["Ext Volt 4"]) {
            voltage4 = parseInt(line.slice(currentChar, currentChar+4), 16) / 13107;
            msg += ' Volt 4=' + voltage4
            currentChar += 4;
        }
        if (voltages["Ext Volt 5"]) {
            console.info('Volt 5', line.slice(currentChar, currentChar+4));
            voltage5 = parseInt(line.slice(currentChar, currentChar+4), 16) / 13107;
            msg += ' Volt 5=' + voltage5
            currentChar += 4;
        }
        if (extraSensors["SBE 38"]) {

            currentChar += 6;
        }
        if (extraSensors["WETLABS"]) {

            currentChar += 12;
        }
        if (extraSensors["Gas Tension Device"]) {
            gtdPressure = parseInt(line.slice(currentChar, currentChar+8), 16) / 100000;
            currentChar += 8;
            gtdTemp = parseInt(line.slice(currentChar, currentChar+6), 16) / 100000 - 10;
            currentChar += 6;
        }        
        if (extraSensors["OPTODE"]) {
            oxygen = parseInt(line.slice(currentChar, currentChar+6), 16) / 10000 - 10;
            currentChar += 6;
            msg += ' OPTODE=' + oxygen
        }
        if (extraSensors["SBE63"]) {
            oxygenPhase = parseInt(line.slice(currentChar, currentChar+6), 16) / 100000 - 10;
            currentChar += 6;
            oxygenTempVoltage = parseInt(line.slice(currentChar, currentChar+6), 16) / 1000000 - 1;
            currentChar += 6;
        }
        console.info(lineno, msg);

    }
    // console.log('Line #' + lineno, line);
});


lineReader.on('close', function() {
    console.log('all done');
    process.exit(0);
});