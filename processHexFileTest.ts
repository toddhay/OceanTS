import { readFileSync, writeFileSync, writeFile, createReadStream } from 'fs';
import { createInterface } from 'readline';
import {Table, Null} from 'apache-arrow';
import fetch from 'node-fetch';
import * as moment from 'moment';
import * as math from 'mathjs';

const line_counter = ((i = 0) => () => ++i)();

const file = "./data/sbe19plusV2/2016_Excalibur/Excalibur_2016_CTD_Leg1/SeabirdOps/PORT_CTD5048_DO1360CT1460Op302_Hauls_1to5_21May2016.hex";

const lineReader = createInterface({ input: createReadStream(file) });

let dataStartLine: number = -1;
let serialNumber: any = null;
let startDateTime: any = null;
let parts: any = null;
let subparts: any = null;
let sensors = {};
let extraSensors = {};
let voltages = {};
let casts = {};
let currentChar = -1;
let t: number, c: number, p: number, ptcv: number = null, oxygen: number,
    oxygenPhase: number, oxygenTempVoltage: number, gtdPressure: number, gtdTemp: number;
let voltage0: number, voltage1:number, voltage2: number, 
    voltage3: number, voltage4:number, voltage5: number;
let cleanLine: string, castNum: string, castStartDate: string,
    castStartEnd: any, castStartNum: number, castEndNum: number;

const parsingRules = [
    {"sensor": "Temperature", "variable": "Temperature A/D Counts", "size": 6, "operations": null},
    {"sensor": "Conductivity", "variable": "Conductivity Frequency", "size": 6, "operations": [{"op": math.divide, "value": 256}]},
    {"sensor": "Pressure", "variable": "Pressure A/D Counts", "size": 6, "operations": null},
    {"sensor": "Pressure", "variable": "Pressure Temperature Compensation Voltage", "size": 4, "operations": [{"op": math.divide, "value": 13107}]},
    {"sensor": "Voltage", "variable": "External Voltage 0", "size": 4, "operations": [{"op": math.divide, "value": 13107}]},
    {"sensor": "Voltage", "variable": "External Voltage 1", "size": 4, "operations": [{"op": math.divide, "value": 13107}]},
    {"sensor": "Voltage", "variable": "External Voltage 2", "size": 4, "operations": [{"op": math.divide, "value": 13107}]},
    {"sensor": "Voltage", "variable": "External Voltage 3", "size": 4, "operations": [{"op": math.divide, "value": 13107}]},
    {"sensor": "Voltage", "variable": "External Voltage 4", "size": 4, "operations": [{"op": math.divide, "value": 13107}]},
    {"sensor": "Voltage", "variable": "External Voltage 5", "size": 4, "operations": [{"op": math.divide, "value": 13107}]},
    {"sensor": "SBE38", "variable": "SBE38 Temperature", "size": 6, "operations": 
        [{"op": math.divide, "value": 100000}, {"op": math.subtract, "value": 10}]},
    {"sensor": "WETLABS", "variable": "WETLABS Signal Counts", "size": 12, "operations": null},
    {"sensor": "GasTensionDevice", "variable": "GTD Pressure", "size": 6, "operations": [{"op": math.divide, "value": 100000}]},                    // ToDo
    {"sensor": "GasTensionDevice", "variable": "GTD Temperature", "size": 6, "operations": 
        [{"op": math.divide, "value": 100000}, {"op": math.subtract, "value": 10}]},
    {"sensor": "OPTODE", "variable": "OPTODE Oxygen", "size": 6, "operations":
        [{"op": math.divide, "value": 10000}, {"op": math.subtract, "value": 10}]},
    {"sensor": "SBE63", "variable": "SBE63 Oxygen Phase", "size": 6, "operations":
        [{"op": math.divide, "value": 100000}, {"op": math.subtract, "value": 10}]},                              
    {"sensor": "SBE63", "variable": "SBE63 Oxygen Temperature Voltage", "size": 6, "operations":
        [{"op": math.divide, "value": 1000000}, {"op": math.subtract, "value": 1}]},
                                                                                        // SeaFET - ToDo
                                                                                        // Time - ToDo
]
let parsedLine = {};
let msg: string = null;
let value: any = null;

lineReader.on('line', (line, lineno = line_counter()) => {

    if ((lineno > dataStartLine) && (dataStartLine !== -1)) lineReader.close();

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
                extraSensors[parts[0].replace(/ /g,'')] = parts[1] === "yes" ? true : false;
            }
        })
    }

    if (line.startsWith("* Ext Volt")) {
        parts = line.split(",").map(s => s.replace("*", "").trim());
        parts.forEach((x: any) => {
            subparts = x.split("=").map(s => s.trim());
            if (subparts.length === 2) 
                voltages[subparts[0].replace("Ext", "External").replace("Volt", "Voltage")] = 
                    subparts[1] === "yes" ? true : false;
        })
    }

    if (line.startsWith("* cast")) {
        // Parse casts
        parts = line.replace("* cast", "").trim().split(",");
        if (parts.length >= 1) {
            subparts = parts[0].split("samples").map(s => s.trim());
            castNum = subparts[0].split(" ")[0];
            castStartDate = subparts[0].replace(castNum, "").trim();
            castStartEnd = subparts[1].split("to").map(s => s.trim());
            castStartNum = parseInt(castStartEnd[0]);
            castEndNum = parseInt(castStartEnd[1]);
            // console.info("cast, num=", castNum,  
            //     castStartDate, castStartNum, castEndNum);
            casts[castNum] = {
                "startDate": castStartDate,
                "startNum": castStartNum,
                "endNum": castEndNum
            }
        }
    }

    if (line.startsWith('*END*')) { 
        dataStartLine = lineno + 1; 

        console.info('sensors: ' + JSON.stringify(sensors));
        console.info('extraSensors: ' + JSON.stringify(extraSensors));
        console.info('voltages: '+ JSON.stringify(voltages));
        console.info('casts: ' + JSON.stringify(casts));
        console.info('dataStartLine: ' + dataStartLine);
    }

    if ((lineno >= dataStartLine) && (dataStartLine !== -1)) {
        // Parse Data
        msg = '';
        parsedLine = {};
        currentChar = 0;

        parsingRules.forEach(rule => {
            if (((rule.sensor.startsWith("Voltage")) && !(voltages[rule.variable])) ||
                    ((rule.sensor in extraSensors) && !(extraSensors[rule.sensor]))) {
                return;
            }
            value = parseInt(line.slice(currentChar, currentChar+rule.size), 16);
            if (rule.operations !== null) {
                rule.operations.forEach(operation => {
                    console.info(rule.variable);
                    console.info('\tvalue before', line.slice(currentChar, currentChar+rule.size), 'convert to', value, operation.op.name, 'by', operation.value);
                    value = operation.op(value, operation.value);
                    console.info('\tvalue after', value)
                });
            } else {
                console.info(rule.variable, line.slice(currentChar, currentChar+rule.size), 'convert to', value);
            }
            parsedLine[rule.variable] = value;
            currentChar += rule.size;
        });
        console.info(JSON.stringify(parsedLine));
    }
    // console.log('Line #' + lineno, line);
});

lineReader.on('close', function() {
    console.log('all done');
    process.exit(0);
});