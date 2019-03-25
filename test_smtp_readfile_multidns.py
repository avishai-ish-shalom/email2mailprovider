#!/usr/bin/env python
import smtplib
import dns.resolver
import dns.exception
import ipdb
import sys
import sqlite3
import time

conn = sqlite3.connect('scanner.db')

my_resolver = dns.resolver.Resolver()
dns_server_list = ['209.244.0.3', '84.200.69.80', '8.26.56.26', '208.67.222.222', '156.154.70.1', '199.85.126.10', '81.218.119.11',
                   '195.46.39.39', '107.150.40.234', '208.76.50.50', '216.146.35.35', '37.235.1.174', '89.233.43.71', '74.82.42.42', '109.69.8.51']
dnscnt = 1

infile = open(sys.argv[1], 'r')

c = conn.cursor()

for line in infile:

    line = line.rstrip()
    xline = [line]

    c.execute('select count(1) from name_to_mx_mapping where name=?', xline)
    a = c.fetchone()

    print("Found %s records for %s" % (a[0], line))

    if (0 == a[0]):
        if (0 == (dnscnt % len(dns_server_list))):
            dnscnt = 1
            time.sleep(1)
        try:
            print(dns_server_list[dnscnt-1])
            my_resolver_nameservers = [dns_server_list[dnscnt-1]]
            dnscnt += 1
            mx_dns_resp = dns.resolver.query(str(line), 'MX')
            for rdata in mx_dns_resp:
                print(line, str(rdata.exchange))
                c.execute('insert into name_to_mx_mapping values (?, ?)',
                          (line, str(rdata.exchange).lower()))
                conn.commit()
                print('Host', rdata.exchange, 'has preference', rdata.preference)
            print(mx_dns_resp[0].exchange.to_text())
        except dns.resolver.NXDOMAIN:
            print('NXdomain handler')
            c.execute('insert into name_to_mx_mapping values (?, ?)',
                      (line, 'NXDOMAIN'))
            conn.commit()
        except (dns.resolver.Timeout, dns.resolver.NoAnswer):
            print('Timeout handler')
            c.execute('insert into name_to_mx_mapping values (?, ?)',
                      (line, 'NoAnswer'))
            conn.commit()
        except dns.exception.DNSException as e:
            print('other exception: %s' % type(e))
            pass
"""
smtp_connection = smtplib.SMTP(mx_dns_resp[0].exchange.to_text(),25)
resp_object = smtp_connection.ehlo()
print "Responses: banner %s, extensions %s" % (smtp_connection.ehlo_resp, smtp_connection.esmtp_features)
smtp_connection.quit()
"""
