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

require_once 'Muuttaa/Exception.php';

/**
 * A query in a {@link Muuttaa_Exception} failed 
 *
 * @package   Muuttaa
 * @author    Joe Stump <joe@joestump.net>
 * @copyright 2008 Digg.com, Inc. 
 * @license   http://tinyurl.com/42zef New BSD License
 * @version   Release: @package_version@
 * @link      http://code.google.com/p/muuttaa
 */
class Muuttaa_Exception_FailedQuery extends Muuttaa_Exception
{
    /**
     * Query that failed
     *
     * @access protected
     * @var string $query The SQL query that failed
     */
    protected $query = '';

    /**
     * Constructor
     *
     * @param string  $query   The SQL query that failed
     * @param string  $message The exception's message, if any
     * @param integer $code    The error code for the exception
     */
    public function __construct($query, $message = null, $code = 0)
    {
        parent::__construct($message, $code);
        $this->query = $query;
    }

    /**
     * Getter for {@link Muuttaa_Exception_FailedQuery::$query}
     * 
     * @return string
     * @see Muuttaa_Exception_FailedQuery::$query
     */
    public function getQuery()
    {
        return $this->query;
    }
}

?>
