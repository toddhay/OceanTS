import { existsSync, mkdirSync } from 'fs';
import * as path from 'path';
// import logjson from './log.json';
import { configure, getLogger } from 'log4js';

const logDir = path.resolve(path.join(__dirname, '../logs'));
if (!existsSync(logDir)) {
    mkdirSync(logDir);
}
export const errorLog = path.resolve(path.join(logDir, 'error.log'));
export const appLog = path.resolve(path.join(logDir, 'app.log'));

let logConfig = {
    "appenders": {
        "app": {
            "type": "dateFile",
            "filename": appLog,
            "pattern": "-yyyy-MM-dd",
            "alwaysIncludePattern": true,
            "keepFileExt": true, 
            "daysToKeep": 15
        },
        "app2": {
            "type": "fileSync",
            "filename": appLog,
            "maxLogSize": 1048576,
            "backups": 100

        },
        "error": {
            "type": "fileSync",
            "filename": errorLog,
            "maxLogSize": 1048576,
            "backups": 100
        },
        "console": { "type": "console" }
    },
    "categories": { 
        "default": { "appenders": [ "app", "console", "app2" ], "level": "debug" },
        "error": { "appenders": [ "error" ], "level": "error" }
    }
};
configure(logConfig);
export const logger = getLogger("default");
export const errorLogger = getLogger("error");
logger.level = "debug";


// import * as winston from 'winston';

// export const logger = winston.createLogger({
//     level: 'info',
//     format: winston.format.combine(
//         winston.format.colorize(),
//         winston.format.json()
//     ),
//     // defaultMeta: { service: 'user-service' },
//     transports: [
//       //
//       // - Write to all logs with level `info` and below to `combined.log` 
//       // - Write all logs error (and below) to `error.log`.
//       //
//       new winston.transports.File({ filename: errorLog.toString(), level: 'error' }),
//       new winston.transports.File({ filename: appLog.toString() }),
//     ]
// });
// if (process.env.NODE_ENV !== 'production') {
//     logger.add(new winston.transports.Console({
//         format: winston.format.simple()
//     }));
// }

