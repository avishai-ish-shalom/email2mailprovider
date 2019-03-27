#!/usr/bin/env python

import dns.resolver, dns.rdatatype
import csv
import itertools
import functools
import sys
from collections import Counter
import click
from concurrent.futures import ThreadPoolExecutor 


def retry(exception, retries=3):
    def decorator(f):
        @functools.wraps(f)
        def func(*args, **kwargs):
            n = 0
            last_exception = None
            while n < retries:
                try:
                    return f(*args, **kwargs)
                except exception as e:
                    n += 1
                    last_exception = e
            else:
                raise last_exception

        return func
    return decorator
        

def company_domain(domain):
    domain = domain.lower().rstrip('.')
    domain_components = domain.split('.')
    tld = domain_components[-1]
    if tld in ['org', 'net', 'com', 'tv']:
        domain_suffix = '.'.join(domain.split('.')[-2:])
        if any(pattern in domain_suffix for pattern in ['google.com', 'googlemail.com']):
            return 'google.com'
        else:
            return domain_suffix
    else:
        return '.'.join(domain.split('.')[-3:])


@retry(dns.resolver.Timeout)
def resolve(domain):
    try:
        answer = dns.resolver.query(domain ,dns.rdatatype.MX)
    except dns.resolver.NXDOMAIN:
        return set(['NO DOMAIN'])
    except dns.resolver.NoAnswer:
        return set(['NO ANSWER'])
    except dns.resolver.NoNameservers:
        return set(['NO NAMESERVERS'])
    mxs = [rdata.exchange.to_text() for rdata in answer]
    return mxs


def mxs_to_provider(mxs):
    domains = set(company_domain(mx) for mx in mxs)
    if len(domains) == 1:
        return domains.pop()
    else:
        print('Multiple providers for domain??\nMX records: {}'.format(mxs), file=sys.stderr)
        return domains.pop()


@click.command()
@click.option('--input', '-i', type=click.File('r'))
@click.option('--output', '-o', type=click.File('w'))
@click.option('--email-column', '-e', help='Email column name', default='email')
@click.option('--threads', '-t', help='Number of resolver threads to use', default=10)
def main(input, output, email_column, threads):
    emails = parse_csv(input, email_column)
    
    with ThreadPoolExecutor(max_workers=threads) as pool:
        resolved = pool.map(lambda email: (email, mxs_to_provider(resolve(domain_from_email(email)))), emails)
    writer = csv.writer(output)
    writer.writerows(resolved)


def domain_from_email(email):
    return email.split('@')[-1]


def parse_csv(csvfile, column):
    reader = csv.DictReader(csvfile)
    return [row[column] for row in reader]


if __name__ == "__main__":
    main()
