package archivist

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/voodooEntity/cyberbrain/src/system/interfaces"
)

const (
	LEVEL_DEBUG   = 1
	LEVEL_INFO    = 2
	LEVEL_WARNING = 3
	LEVEL_ERROR   = 4
	LEVEL_FATAL   = 5
)

// Constants for granular debug levels
const (
	DEBUG_LEVEL_TRACE  = iota + 1 // For tracing execution flow
	DEBUG_LEVEL_INFO              // For informational debug messages
	DEBUG_LEVEL_DETAIL            // For more detailed output
	DEBUG_LEVEL_DUMP              // For dumping entire data structures
	DEBUG_LEVEL_MAX               // The highest, most detailed level
)

type Archivist struct {
	logLevel   map[string]int
	logFlags   [5]bool
	logger     interfaces.LoggerInterface
	debugLevel int
}

type Config struct {
	Logger     interfaces.LoggerInterface
	LogLevel   int
	DebugLevel int
}

func New(conf *Config) *Archivist {
	// init archivist with default log flag set
	archivist := &Archivist{
		logFlags: [5]bool{false, true, true, true, true},
	}

	// in case no logger is given we gonne default
	// to logger to stdout
	archivist.SetLogger(conf.Logger)

	// set the provided loglevel
	archivist.SetLogLevel(conf.LogLevel)

	// set the provided debug level verbosity only if the log level is debug
	if conf.LogLevel == LEVEL_DEBUG {
		archivist.SetDebugLevel(conf.DebugLevel)
	}

	return archivist
}

func (a *Archivist) store(message string, stype string, dump bool, formatted bool, params []interface{}) {
	// dispatch the caller file+line number
	_, file, line, _ := runtime.Caller(2)
	arrPackagePath := strings.Split(file, "/")
	packageFile := arrPackagePath[len(arrPackagePath)-1]

	logLine := time.Now().Format("2006-01-02 15:04:05") + "|" + stype + "|" + packageFile + "#" + strconv.Itoa(line) + "|"
	if true == dump {
		if true == formatted {
			logLine = logLine + fmt.Sprintf(message, params...)
		} else {
			logLine = logLine + message + "|" + fmt.Sprintf("%+v", params)
		}
	} else {
		logLine = logLine + message
	}

	a.logger.Println(logLine)
}

func (a *Archivist) Error(message string, params ...interface{}) {
	if a.logFlags[LEVEL_ERROR-1] {
		if 0 == len(params) {
			a.store(message, "error", false, false, nil)
		} else {
			a.store(message, "error", true, false, params)
		}
	}
}

func (a *Archivist) ErrorF(message string, params ...interface{}) {
	if a.logFlags[LEVEL_ERROR-1] {
		a.store(message, "error", true, true, params)
	}
}

func (a *Archivist) Fatal(message string, params ...interface{}) {
	if a.logFlags[LEVEL_FATAL-1] {
		if 0 == len(params) {
			a.store(message, "fatal", false, false, nil)
		} else {
			a.store(message, "fatal", true, false, params)
		}
	}
}

func (a *Archivist) FatalF(message string, params ...interface{}) {
	if a.logFlags[LEVEL_FATAL-1] {
		a.store(message, "fatal", true, true, params)
	}
}

func (a *Archivist) Info(message string, params ...interface{}) {
	if a.logFlags[LEVEL_INFO-1] {
		if 0 == len(params) {
			a.store(message, "info", false, false, nil)
		} else {
			a.store(message, "info", true, false, params)
		}
	}
}

func (a *Archivist) InfoF(message string, params ...interface{}) {
	if a.logFlags[LEVEL_INFO-1] {
		a.store(message, "info", true, true, params)
	}
}

func (a *Archivist) Warning(message string, params ...interface{}) {
	if a.logFlags[LEVEL_WARNING-1] {
		if 0 == len(params) {
			a.store(message, "warning", false, false, nil)
		} else {
			a.store(message, "warning", true, false, params)
		}
	}
}

func (a *Archivist) WarningF(message string, params ...interface{}) {
	if a.logFlags[LEVEL_WARNING-1] {
		a.store(message, "warning", true, true, params)
	}
}

func (a *Archivist) Debug(level int, message string, params ...interface{}) {
	if a.logFlags[LEVEL_DEBUG-1] && level <= a.debugLevel {
		if 0 == len(params) {
			a.store(message, "debug", false, false, nil)
		} else {
			a.store(message, "debug", true, false, params)
		}
	}
}

func (a *Archivist) DebugF(level int, message string, params ...interface{}) {
	if a.logFlags[LEVEL_DEBUG-1] && level <= a.debugLevel {
		a.store(message, "debug", true, true, params)
	}
}

func (a *Archivist) SetLogLevel(logLevel int) {
	// check for non initialized log level first
	if 0 == logLevel {
		logLevel = LEVEL_WARNING
	}

	if logLevel >= LEVEL_DEBUG && logLevel <= LEVEL_FATAL {
		for index, _ := range a.logFlags {
			if logLevel-1 <= index {
				a.logFlags[index] = true
			} else {
				a.logFlags[index] = false
			}
		}
	} else {
		a.Error("Given LOG_LEVEL is unknown, defaulting to LEVEL_WARNING provided was: ", logLevel)
		a.SetLogLevel(LEVEL_WARNING)
	}
}

func (a *Archivist) SetDebugLevel(level int) {
	if level < 0 {
		level = 0
	}
	a.debugLevel = level
}

func (a *Archivist) SetLogger(logger interfaces.LoggerInterface) {
	// if logger is nil
	if nil == logger {
		logger = log.New(os.Stdout, "", 0)
	}
	//
	a.logger = logger
}
