#!/usr/bin/env python

import dbx_config  # Contains Dropbox API Key & Secret
import json
import sys
from dropbox import DropboxOAuth2FlowNoRedirect


def main():
    auth_flow = DropboxOAuth2FlowNoRedirect(dbx_config.APP_KEY, dbx_config.APP_SECRET)

    authorize_url = auth_flow.start()
    print "1. Go to:", authorize_url
    print "2. Click \"Allow\" (you might have to log in first)."
    print "3. Copy the authorization code."

    org = raw_input("Enter the organization's folder name: ").strip()
    auth_code = raw_input("Enter the authorization code here: ").strip()

    try:
        oauth_result = auth_flow.finish(auth_code)
        token = oauth_result.access_token
        print "User's Token:", token
    except Exception, e:
        print "Error:", e
        sys.exit(1)

    # Read in JSON file
    with open('dbx_org_sync_data.json', 'r') as in_json_datafile:
        org_data = json.load(in_json_datafile)

    org_data[org] = token

    # Write JSON back with new organization's token
    with open('dbx_org_sync_data.json', 'w') as out_json_datafile:
        json.dump(
            org_data,
            out_json_datafile,
            sort_keys=True,
            indent=4,
            separators=(',', ': ')
        )
# end def main()


if __name__ == "__main__":
    main()
