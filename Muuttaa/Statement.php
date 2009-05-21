<?php

/**
 * Muuttaa is a MySQL manipulation queue 
 *
 * PHP version 5.2.0+
 *
 * LICENSE: This source file is subject to the New BSD license that is 
 * available through the world-wide-web at the following URI:
 * http://www.opensource.org/licenses/bsd-license.php. If you did not receive  
 * a copy of the New BSD License and are unable to obtain it through the web, 
 * please send a note to license@php.net so we can mail you a copy immediately.
 *
 * @package   Muuttaa
 * @author    Joe Stump <joe@joestump.net>
 * @copyright 2008 Digg.com, Inc. 
 * @license   http://tinyurl.com/42zef New BSD License
 * @version   SVN: @package_version@
 * @link      http://code.google.com/p/muuttaa
 */

require_once 'Muuttaa.php';
require_once 'Muuttaa/Exception.php';

/** 
 * Muuttaa statement class 
 * 
 * A Muuttaa statement is an SQL transaction that contains one or more queries
 * that is to be ran against one or more DB hosts. A statement can also be 
 * retried multiple times.
 * 
 * @package   Muuttaa
 * @author    Joe Stump <joe@joestump.net>
 * @copyright 2008 Digg.com, Inc. 
 * @license   http://tinyurl.com/42zef New BSD License
 * @version   Release: @package_version@
 * @link      http://code.google.com/p/muuttaa
 */
class Muuttaa_Statement
{
    /**
     * Statement status codes
     *
     * @var integer STATUS_PENDING    Statement is awaiting to be processed
     * @var integer STATUS_COMPLETE   Statement was successfully ran
     * @var integer STATUS_FAILED     Statement has failed
     * @var integer STATUS_EXCEPTION  Statement is in an unknown fatal state
     */
    const STATUS_PENDING   = 1;
    const STATUS_COMPLETE  = 2;
    const STATUS_FAILED    = 4;
    const STATUS_EXCEPTION = 3;

    /**
     * Queries in this statement
     *
     * @access protected
     * @var array $queries An array of queries belonging to this statement
     * @see Muuttaa_Statement::addQuery()
     */
    protected $queries = array();

    /**
     * Hosts the queries need to be ran against
     *
     * @access protected
     * @var array $hosts An array of hosts to run the query against
     * @see Muuttaa_Statement::addHost()
     */
    protected $hosts = array();
        
    /**
     * Number of times to attempt the statement
     *
     * @access protected
     * @var integer $retries Number of attempts
     */
    protected $retries = 0;

    /**
     * Add a host to the statement
     *
     * @param object $host A valid Muuttaa host
     * 
     * @access public
     * @return boolean True if all went well
     * @throws {@link Muuttaa_Exception} if the host is invalid 
     */
    public function addHost($host)
    {
        if (!Muuttaa::isValidHost($host)) {
            throw new Muuttaa_Exception('Host appears to be invalid');
        }

        $cnt = count($this->hosts);
        $res = array_push($this->hosts, $host);
        if ($res != ($cnt + 1)) {
            throw new Muuttaa_Exceptoin('Could not append host');
        }

        return true;
    }

    /**
     * Add a query to the statement
     *
     * @param string $query A valid SQL query to add to this transaction
     * 
     * @access public
     * @throws {@link Muuttaa_Exception} if query was not added
     * @return boolean True if all went well
     */
    public function addQuery($query)
    {
        $cnt = count($this->queries);
        $res = array_push($this->queries, $query);
        if ($res != ($cnt + 1)) {
            throw new Muuttaa_Exceptoin('Could not append statement');
        }

        return true;
    }

    /**
     * Set the number of retries
     *
     * @param integer $retries Number of times to retry the statement
     *
     * @return void
     */
    public function setRetries($retries)
    {
        $this->retries = (int)$retries;
    }

    /**
     * Get all hosts
     * 
     * @access public
     * @return array The list of hosts for this statement
     */
    public function getHosts()
    {
        return $this->hosts;
    }

    /**
     * Get all queries
     * 
     * @access public
     * @return array The list of queries for this statement
     */
    public function getQueries()
    {
        return $this->queries;
    }

    /**
     * Get number of retries
     * 
     * @access public
     * @return integer Number of times to retry this statement
     */
    public function getRetries()
    {
        return (int)$this->retries;
    }
}

?>
