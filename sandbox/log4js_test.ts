import { configure, getLogger, shutdown } from 'log4js';
import logjson from './src/log.json';

export const logger = getLogger();
configure(logjson);
logger.level = "debug";

logger.info('start capturing logs....');

logger.error(`really bad error`);

setInterval(function(){ process.exit(0); }, 100);
