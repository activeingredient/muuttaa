<?php

/**
 * A class to run Muuttaa from the CLI
 * 
 * PHP version 5.2+
 * 
 * This CLI processor was modified from an original version, which is part of 
 * the PHP_CodeSniffer package. As a result, everything below in the license,
 * author, etc. blocks reflect the base authors.
 *
 * @package   Muuttaa
 * @author    Greg Sherwood <gsherwood@squiz.net>
 * @author    Joe Stump <joe@joestump.net> 
 * @copyright 2006 Squiz Pty Ltd (ABN 77 084 670 600)
 * @license   http://matrix.squiz.net/developer/tools/php_cs/licence BSD Licence
 * @version   SVN: @package_version@
 * @link      http://pear.php.net/package/PHP_CodeSniffer
 */

require_once 'Muuttaa/Process.php';

/**
 * A class to run Muuttaa from the CLI
 *
 * @package   Muuttaa
 * @author    Greg Sherwood <gsherwood@squiz.net>
 * @author    Joe Stump <joe@joestump.net> 
 * @copyright 2006 Squiz Pty Ltd (ABN 77 084 670 600)
 * @license   http://matrix.squiz.net/developer/tools/php_cs/licence BSD Licence
 * @version   Release: @package_version@
 * @link      http://pear.php.net/package/PHP_CodeSniffer
 */
class Muuttaa_CLI
{
    /**
     * Get a list of default values for all possible command line arguments.
     *
     * - names      The names of the Muuttaa queues to process
     * - dsn        The DSN's where the queues live. DSN's are in the form of
     *              PEAR DB's (e.g. mysql://root:pwd@localhost/mydbname).
     *
     * @access public
     * @return array
     */
    public function getDefaults()
    {
        // The default values for config settings.
        $defaults['names'] = array();
        $defaults['dsn']   = array();
    }

    /**
     * Process the command line arguments and returns the values.
     *
     * @access public
     * @return array
     */
    public function getCommandLineValues()
    {
        $values = $this->getDefaults();
        for ($i = 1; $i < $_SERVER['argc']; $i++) {
            $arg = $_SERVER['argv'][$i];
            if ($arg{0} === '-') {
                if ($arg{1} === '-') {
                    $values = $this->processLongArgument(substr($arg, 2), $i, $values);
                } else {
                    $switches = str_split($arg);
                    foreach ($switches as $switch) {
                        if ($switch === '-') {
                            continue;
                        }

                        $values = $this->processShortArgument($switch, $i, $values);
                    }
                }
            } else {
                $values = $this->processUnknownArgument($arg, $i, $values);
            }
        }

        return $values;
    }


    /**
     * Processes a sort (-e) command line argument.
     *
     * @param string  $arg    The command line argument.
     * @param integer $pos    The position of the argument on the command line.
     * @param array   $values An array of values determined from CLI args.
     *
     * @return array The updated CLI values.
     * @see getCommandLineValues()
     */
    public function processShortArgument($arg, $pos, $values)
    {
        static $map = array(
            'i' => 'iterate',
            'w' => 'wait',
            's' => 'statements',
            'm' => 'multiplier'
        );

        switch ($arg) {
        case 'h':
        case '?':
            $this->printUsage();
            exit(0);
            break;
        case 'v':
            $this->printVersion();
            exit(0);
            break;
        case 'w':
        case 'i':
        case 's':
        case 'm':
            if (!isset($_SERVER['argv'][($pos + 1)])) {
                echo 'ERROR: A value is required for "' . $map[$arg] . '"' . PHP_EOL . PHP_EOL;
                $this->printUsage();
                exit(2);
            }

            $val = $_SERVER['argv'][($pos + 1)]; 

            if (!is_numeric($val)) {
                echo 'ERROR: A "' . $map[$arg] . '" with a value of "' . $val . '" is invalid.' . PHP_EOL . PHP_EOL;
                $this->printUsage();
                exit(2);
            }
 
            $values[$map[$arg]] = $val;
            break;
        case 'd':
            $values['dsn'][] = $_SERVER['argv'][($pos + 1)];
            break;
        case 'q' :
            $values['queues'][] = $_SERVER['argv'][($pos + 1)];
            break;
        }

        return $values;
    }

    /**
     * Processes a long (--example) command line argument.
     *
     * @param string $arg    The command line argument.
     * @param int    $pos    The position of the argument on the command line.
     * @param array  $values An array of values determined from CLI args.
     *
     * @return array The updated CLI values.
     * @see getCommandLineValues()
     */
    public function processLongArgument($arg, $pos, $values)
    {
        switch ($arg) {
        case 'help':
            $this->printUsage();
            exit(0);
            break;
        case 'version':
            $this->printVersion();
            exit(0);
            break;
        default:
            if (!strpos($arg, '=')) {
                return $values;
            }

            list($name, $val) = explode('=', $arg);
            switch ($name) {
            case 'host':
            case 'queue': 
                $key = $name . 's';
                if (!isset($values[$key])) {
                    echo 'ERROR: option "' . $name . '" not known.' . PHP_EOL . PHP_EOL;
                    $this->printUsage();
                    exit(2);
                }
            case 'iterate':
            case 'statements':
            case 'multiplier':
            case 'wait':
                if (!is_numeric($val)) {
                    echo 'ERROR: A "' . $name . '" with a value of "' . $name . '" is invalid.' . PHP_EOL . PHP_EOL;
                    $this->printUsage();
                    exit(2);
                }
        
                $values[$name] = (int)$val;
            }

            $values[$key][] = $val;
            break;
        }

        return $values;
    }

    /**
     * Processes an unknown command line argument.
     *
     * Assumes all unknown arguments are files and folders to check.
     *
     * @param string $arg    The command line argument.
     * @param int    $pos    The position of the argument on the command line.
     * @param array  $values An array of values determined from CLI args.
     *
     * @return array The updated CLI values.
     * @see getCommandLineValues()
     */
    public function processUnknownArgument($arg, $pos, $values)
    {
        return $values;
    }

    /**
     * Process the Muuttaa queue
     *
     * Takes the various CLI arguments and starts munching through the queues
     * specified at the rate specified. 
     *
     * @return void
     * @see getCommandLineValues(), iterate()
     */
    public function process()
    {
        $values = $this->getCommandLineValues();

        if (!count($values['queues'])) {
            echo 'ERROR: One or more queues are required.' . PHP_EOL . PHP_EOL;
            $this->printUsage();
            exit(2);
        }

        if (!isset($values['dsn']) || 
            !is_array($values['dsn']) || 
            !count($values['dsn'])) {
            echo "ERROR: One or more DSN's are required." . PHP_EOL . PHP_EOL;
            $this->printUsage();
            exit(2);
        }

        $req   = array('scheme', 'host', 'user', 'path');
        $hosts = array();
        foreach ($values['dsn'] as $dsn) {
            $invalid = false;
            $parts   = parse_url($dsn);
            if ($parts === false) {
                $invalid = true;
            } else {
                foreach ($req as $key) {
                    if (!isset($parts[$key])) {
                        $invalid = true;
                    }
                }
            }

            if ($invalid) {
                echo 'ERROR: DSN "' . $dsn . '" appears to be invalid.'  . PHP_EOL . PHP_EOL;
                $this->printUsage();
                exit(2);
            }

            $host       = new stdClass;
            $host->type = $parts['scheme'];
            $host->host = $parts['host'];
            $host->user = $parts['user'];
            $host->port = isset($parts['port']) ? $parts['port'] : '3306'; 
            $host->pass = isset($parts['pass']) ? $parts['pass'] : ''; 
            $host->name = substr($parts['path'], 1);
            $hosts[]    = $host;
        }

        $check = array('iterate', 'statements', 'wait');
        foreach ($check as $key) {
            if (!isset($values[$key]) || !is_numeric($values[$key])) {
                echo 'ERROR: '. $key . ' not specified or invalid.'  . 
                     PHP_EOL . PHP_EOL;
                $this->printUsage();
                exit(2);
            }
        }

        if ($values['iterate'] == 0) {
            while (true) {
                $this->iterate($values['queues'], 
                               $hosts,
                               $values['statements'], 
                               $values['wait'],
                               $values['multiplier']);
            }
        } else {
            for ($i = 0 ; $i < $values['iterate'] ; $i++) {
                $this->iterate($values['queues'], 
                               $hosts,
                               $values['statements'], 
                               $values['wait'],
                               $values['multiplier']);
            }
        }
    }

    /**
     * Run a single iteration
     *
     * @param array   $queues     An array of queues to munch on
     * @param array   $hosts      An array of Muuttaa hosts
     * @param integer $statements Number of statements to bite off
     * @param integer $wait       How long to wait after this iteration 
     * @param float   $multiplier Used by Muuttaa to dynamically rate limit
     *
     * @return void
     * @see Muuttaa_Process, Muuttaa_Process::process()
     */
    protected function iterate(array $queues, 
                               array $hosts, 
                               $statements, 
                               $wait, 
                               $multiplier)
    {
        $muuttaa = new Muuttaa_Process($queues, $hosts);
        $muuttaa->process($statements, $multiplier);
        sleep($wait);
    }

    /**
     * Prints out the usage information for this script.
     *
     * @return void
     */
    public function printUsage()
    {
        echo 'Usage: muuttaa [-qd] [--queue=<queue>] [--dsn=<dsn>]' . PHP_EOL;
        echo '        -q, --queue=QUEUE Muuttaa queue name(s) to process' . PHP_EOL;
        echo '        -d, --dsn=DSN     PDB DSN for Muuttaa host(s)' . PHP_EOL;
        echo '                          (e.g. mysql://root:pwd@localhost/dbname)' . PHP_EOL;
        echo '        -h, --help        Print this help message'. PHP_EOL;
        echo '        -v, --version     Print version information' . PHP_EOL;
        echo '        -i, --iterate     Number of iterations (0 = continuous) ' . PHP_EOL;
        echo '        -s, --statements  Number of statements per iteration' . PHP_EOL;
        echo '        -w, --wait        Seconds to wait between iterations' . PHP_EOL;
        echo '        -m, --mulitplier  Multiplier for delay between statements' . PHP_EOL;
    }

    /**
     * Echo out the version information
     *
     * @access protected
     * @return void
     */
    protected function printVersion()
    {
        echo '@package_name@ version @package_version@ (@package_state@) ';
        echo 'by Digg Inc. (http://digg.com)' . PHP_EOL;
    }
}

?>
