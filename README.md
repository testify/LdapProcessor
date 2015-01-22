LdapProcessor
========================
*Testify Processor for interacting with the LDAP Protocol*
# Usage
### LdapProcessor
  *Binds to a running ldap instance defined in `<endpoint></endpoint>`*
####Parameters
  *The following parameters are supported in the within `<test></test>`*

* **operation:**   _LDAP Operation to perform (Supported values: add, modify delete)_
* **bindDn:**       _DN of for a user to perform LDAP operations_
* **password:**     _Password corresponding to the user defined in `bindDN`_
* **file:**         _Ldif file to be used_

## Example
### LDAP Add

    <testcase>
      <type>LdapProcessor</type>
      <endpoint>localhost:389</endpoint>
        <test>
          <operation>add</operation>
          <bindDn>cn=admin,ou=users,dc=example,dc=com</bindDn>
          <password>admin</password>
          <file>foo.ldif</file>
        </test>
    </testcase>

*add.ldif*

    dn: uid=testuser1,ou=users,dc=example,dc=com
    objectClass: top
    objectClass: extensibleObject
    objectClass: inetOrgPerson
    objectClass: organizationalPerson
    objectClass: person
    cn: Test User
    sn: testuser1
    givenName: Test User
    mail: testuser1@example.com
    uid: testuser1
