#!/bin/bash

source /tmp/jclient_token_`id -u`

(
echo "<document>"
echo "<command>password</command><o>$Passwd</o>"
echo "</document>"


echo "<document>"
echo "<command>ls</command>"
echo "<o>-l</o>"
echo "<o>/alice/data/</o>"
echo "</document>"
) | nc -q 5 localhost $Port

