import { createReadStream } from 'fs';
import * as stream from 'stream';
import { createInterface } from 'readline';
import { Table, FloatVector, Dictionary } from 'apache-arrow';
import * as moment from 'moment-timezone';
import * as math from 'mathjs';
import { hex2dec, counts2frequency } from '../utilities';
import { convertToEngineeringUnits } from './convertResults';


export function parseHex(hexFile: string, instrument: Dictionary, coefficients: Dictionary[],
                         hauls?: Table) {
    /*
    hexFile: string - full path to a Seabird 19plusV2 hex file
    */

    let dataStartLine: number = -1, lineNum: number = -1;
    let serialNumber: any = null;
    let endDateTime: any = null;
    let samples: number = null;
    let parts: any = null;
    let subparts: any = null;
    let pressureSensor = {}, extraSensors = {}, voltages = {}, casts = [];
    let castNum: string, castStartDate: Date, castStartEnd: Date, castStartNum: number, castEndNum: number, castAvg: number;
    let voltageOffsets = {}, tempVoltage: string = "", pumpDelay: number = null;

    const parsingRules = [
        {"sensor": "Temperature", "variable": "Temperature A/D Counts", "size": 6, "data": null, "isADCount": true, "operations": null},
        {"sensor": "Conductivity", "variable": "Conductivity Frequency", "size": 6, "data": null, 
            "operations": [{"op": math.divide, "value": 256}]},
        {"sensor": "Pressure", "variable": "Pressure A/D Counts", "size": 6, "data": null, "isADCount": true, "operations": null},
        {"sensor": "Pressure", "variable": "Pressure Temperature Compensation Voltage", "size": 4, "data": null, 
            "operations": [{"op": math.divide, "value": 13107}]},
        {"sensor": "Voltage", "variable": "External Voltage 0", "size": 4, "data": null, "operations": [{"op": math.divide, "value": 13107}]},
        {"sensor": "Voltage", "variable": "External Voltage 1", "size": 4, "data": null, "operations": [{"op": math.divide, "value": 13107}]},
        {"sensor": "Voltage", "variable": "External Voltage 2", "size": 4, "data": null, "operations": [{"op": math.divide, "value": 13107}]},
        {"sensor": "Voltage", "variable": "External Voltage 3", "size": 4, "data": null, "operations": [{"op": math.divide, "value": 13107}]},
        {"sensor": "Voltage", "variable": "External Voltage 4", "size": 4, "data": null, "operations": [{"op": math.divide, "value": 13107}]},
        {"sensor": "Voltage", "variable": "External Voltage 5", "size": 4, "data": null, "operations": [{"op": math.divide, "value": 13107}]},
        {"sensor": "SBE38", "variable": "SBE38 Temperature", "size": 6, "data": null, "operations": 
            [{"op": math.divide, "value": 100000}, {"op": math.subtract, "value": 10}]},
        {"sensor": "WETLABS", "variable": "WETLABS Signal Counts", "size": 12, "data": null, "operations": null},
        {"sensor": "GasTensionDevice", "variable": "GTD Pressure", "size": 6, "data": null, 
            "operations": [{"op": math.divide, "value": 100000}]},
        {"sensor": "GasTensionDevice", "variable": "GTD Temperature", "size": 6, "data": null,
            "operations": [{"op": math.divide, "value": 100000}, {"op": math.subtract, "value": 10}]},
        {"sensor": "OPTODE", "variable": "OPTODE Oxygen", "size": 6, "data": null,
            "operations": [{"op": math.divide, "value": 10000}, {"op": math.subtract, "value": 10}]},
        {"sensor": "SBE63", "variable": "SBE63 Oxygen Phase", "size": 6, "data": null,
            "operations": [{"op": math.divide, "value": 100000}, {"op": math.subtract, "value": 10}]},                              
        {"sensor": "SBE63", "variable": "SBE63 Oxygen Temperature Voltage", "size": 6, "data": null, 
            "operations": [{"op": math.divide, "value": 1000000}, {"op": math.subtract, "value": 1}]},
        {"sensor": "SeaFET", "variable": "SeaFET Internal Reference Cell Voltage", "size": 7, "data": null,
            "operations": [{"op": math.divide, "value": 1000000}, {"op": math.subtract, "value": 8}]},
        {"sensor": "SeaFET", "variable": "SeaFET External Reference Cell Voltage", "size": 12, "data": null,
            "operations": [{"op": math.divide, "value": 1000000}, {"op": math.subtract, "value": 8}]},
        {"sensor": "Clock", "variable": "Time, Seconds since January 1, 2000", "size": 8, "data": null, "operations": null}
    ]
    let currentChar: number = 0, dataRow: number = 0, value: any = null, schema = [];
    let df = null;

    let start = moment();

    // const output = new stream.PassThrough({ objectMode: true });
    let output: any = null;

    const line_counter = ((i = 0) => () => ++i)();
    const lineReader = createInterface({ input: createReadStream(hexFile) });

    lineReader.on('line', (line, lineNum = line_counter()) => {

        // if ((lineNum > dataStartLine) && (dataStartLine !== -1)) lineReader.close();

        if ((line.startsWith('* SBE 19plus V 2.5.2')) && (endDateTime === null)) {
            // Parse the ending date/time and serial number
            const lineParts = line.split("SERIAL NO.").map(s => s.trim());
            if (lineParts.length === 2) {
                serialNumber = lineParts[1].split(' ')[0];
                endDateTime = lineParts[1].replace(serialNumber, "").trim()
                console.info('\tendDateTime: ' + endDateTime);
            }
        }

        if (line.startsWith("* samples")) {
            parts = line.split(",").map(s => s.replace("*", "").trim());
            subparts = parts[0].split("=")
            samples = parseInt(subparts[1]);
        }

        if (line.startsWith("* mode =")) {
            parts = line.split(", ");
            if (parts.length === 3) {
                pumpDelay = parseFloat(parts[2].replace("pump delay =", "").replace("sec", ""))
            }
        }

        // ToDo - Parse autorun + magnetic modee

        if (line.startsWith("* pressure sensor")) {
            // Parse pressure sensor information
            parts = line.split(",").map(s => s.replace("*", "").trim());
            parts.forEach((x: any) => {
                subparts = x.split("=").map(s => s.trim());
                if (subparts.length === 2) 
                    pressureSensor[subparts[0]] = subparts[1];
            })
        }

        if (line.startsWith("* SBE 38")) {
            // Parse for determining if extra sensors exist or not
            parts = line.split(",");
            parts.forEach((x: any) => {
                subparts = x.split("=").map(s => s.replace("*", "").trim());
                if (subparts.length === 2) {
                    extraSensors[subparts[0].replace(/ /g,'')] = subparts[1] === "yes" ? true : false;
                }
            })
        }

        if (line.startsWith("* Ext Volt")) {
            // Parse extra voltage data
            parts = line.split(",").map(s => s.replace("*", "").trim());
            parts.forEach((x: any) => {
                subparts = x.split("=").map(s => s.trim());
                if (subparts.length === 2) 
                    voltages[subparts[0].replace("Ext", "External").replace("Volt", "Voltage")] = 
                        subparts[1] === "yes" ? true : false;
            })
        }

        if ((line.startsWith("* volt ")) && (line.includes("offset ="))) {
            parts = line.split(":").map(s => s.replace("* volt", "Voltage".trim()));
            tempVoltage = parts[0];
            if (parts.length === 2) {
                subparts = parts[1].split(",");
                voltageOffsets[tempVoltage] = {
                    "offset": parseFloat(subparts[0].replace("offset =", "").trim()),
                    "slope": parseFloat(subparts[1].replace("slope =", "").trim())
                }
            }
        }

        if (line.startsWith("* cast")) {
            // Parse casts
            parts = line.replace("* cast", "").trim().split(",");
            if (parts.length >= 1) {
                subparts = parts[0].split("samples").map(s => s.trim());
                castNum = subparts[0].split(" ")[0];
                // console.info(`start Date: ${subparts[0].replace(castNum, "").trim()}`);
                castStartDate = moment(subparts[0].replace(castNum, "").trim(), "DD MMMM YYYY HH:mm:ss").tz('America/Los_Angeles').toDate();
                castStartEnd = subparts[1].split("to").map(s => s.trim());
                castStartNum = parseInt(castStartEnd[0]);
                castEndNum = parseInt(castStartEnd[1]);
                castAvg = parseInt(parts[1].split("=").map(s => s.trim())[1]);
                casts.push({
                    "cast": parseInt(castNum),
                    "startDate": castStartDate,
                    "startNum": castStartNum,
                    "endNum": castEndNum,
                    "avg": castAvg
                });
            }
        }

        if (line.startsWith('*END*')) {
            // Parse where the data lines start
            dataStartLine = lineNum + 1; 
        }

        if ((lineNum >= dataStartLine) && (dataStartLine !== -1)) {
            
            // Parse Data
            currentChar = 0;
            value = null;

            parsingRules.forEach(rule => {
                if (((rule.sensor.startsWith("Voltage")) && !(voltages[rule.variable])) ||
                    ((rule.sensor in extraSensors) && !(extraSensors[rule.sensor]))) {
                    return;
                }
                value = hex2dec(line.slice(currentChar, currentChar+rule.size));
                if ((rule.operations !== null) && !(isNaN(value))) {
                    rule.operations.forEach((operation: any) => {
                        value = operation.op(value, operation.value);   // Perform the rule math operation on the value
                    });
                }
                if ((value !== null) && !(isNaN(value))) {
                    if (lineNum === dataStartLine) {
                        schema.push(rule.variable)
                        rule.data = new Float32Array(samples);
                    }
                    rule.data[dataRow] = value;
                    currentChar += rule.size;
                }
            });
            dataRow += 1;
        }
        lineNum += 1;
    });

    lineReader.on('close', function() {
        let end = moment();
        let duration = moment.duration(end.diff(start)).asSeconds();
        console.info(`\tProcessing time - parsing hex file: ${duration}s`);

        let dataArrays = [];
        let tempArray: any = null;
        schema.forEach(x => {
            tempArray = parsingRules.find(y => y.variable === x);
            dataArrays.push(FloatVector.from(tempArray.data));
        })
        df = Table.new(dataArrays, schema);
        convertToEngineeringUnits(instrument, coefficients, casts, voltageOffsets, pumpDelay, df, hauls);
    });
}
