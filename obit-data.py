import csv
import requests

from bs4 import BeautifulSoup
from time import sleep

'''
    This program extracts the data from the Chattanooga Public Library's OAI-PMH Repository, which contains
    records of obituaries from the Chattanooga Times Free Press, into a Socrata dataset.
'''

# Open the CSV file to write to as the data comes in
with open(
    "obit_data.csv", "wt", newline="", encoding="utf-8"
) as csv_file:

    # Create the writer and write the header for the dataset
    writer = csv.DictWriter(
        csv_file, 
        fieldnames = [
            "title",
            "publisher",
            "date",
            "type",
            "audience",
            "abstract",
            "isPartOf",
            "identifier"
        ]
    )
    writer.writeheader()

    # Close the file after writing the header
    csv_file.close()

# This is the base URL for each query
base_url = "https://collections.chattlibrary.org/s/obits/oai?verb=ListRecords"

# Empty to begin with - each request will return a value that is used in the next request
resumptionToken = None

print("Beginning scrape. This could take several hours.")

# There is a limit of 50 records per request, so the data fetch must be paginated
for i in range(100_000):

    print(f"----------------------------------------\nPage Number: {i + 1}")

    # Construct the URL for the request, adding the right resumptionToken from the second request onward
    if resumptionToken is not None:
        url = base_url + f"&resumptionToken={resumptionToken}"
    else:
        url = base_url + "&metadataPrefix=oai_dcterms"

    for retry in range(3):

        retries_remaining = 2 - retry

        print(f"Attempt {retry + 1} - {retries_remaining} retries remaining.")

        try: 
            
            # Sleep for 5 seconds, then issue the request and get the XML response
            sleep(5)
            response = requests.get(url)

            # Create a BeautifulSoup object out of the XML in order to parse it easily
            soup = BeautifulSoup(response.content, "xml")
            soup.prettify()

            # Store the resumptionToken from this request for the next one
            rt = soup.find("resumptionToken")
            if rt is not None:
                resumptionToken = rt.text

            # From the soup, pick each record's metadata and make it into a data row (dictionary)
            records = soup.css.select("metadata")
            for record in records:
                row = {}

                # title
                title = record.find("dcterms:title")
                if title is not None:
                    row["title"] = title.text
                else:
                    row["title"] = None

                # publisher
                publisher = record.find("dcterms:publisher")
                if publisher is not None:
                    row["publisher"] = publisher.text
                else:
                    row["publisher"] = None

                # date
                date = record.find("dcterms:date")
                if date is not None:
                    row["date"] = date.text
                else:
                    row["date"] = None

                # type
                type = record.find("dcterms:type")
                if type is not None:
                    row["type"] = type.text
                else:
                    row["type"] = None

                # audience
                audience = record.find("dcterms:audience")
                if audience is not None:
                    row["audience"] = audience.text
                else:
                    row["audience"] = None

                # abstract
                abstract = record.find("dcterms:abstract")
                if abstract is not None:
                    row["abstract"] = abstract.text
                else:
                    row["abstract"] = None

                # isPartOf
                isPartOf = record.find("dcterms:isPartOf")
                if isPartOf is not None:
                    row["isPartOf"] = isPartOf.text
                else:
                    row["isPartOf"] = None

                # identifier
                identifier = record.find("dcterms:identifier")
                if identifier is not None:
                    row["identifier"] = identifier.text
                else:
                    row["identifier"] = None

                # Open the CSV file to write to as the data comes in
                with open(
                    "obit_data.csv", "a", newline="", encoding="utf-8"
                ) as csv_file:

                    # Create the writer and write the header for the dataset
                    writer = csv.DictWriter(
                        csv_file, 
                        fieldnames = [
                            "title",
                            "publisher",
                            "date",
                            "type",
                            "audience",
                            "abstract",
                            "isPartOf",
                            "identifier"
                        ]
                    )
                    writer.writerow(row)

                    # Close the file after writing the header
                    csv_file.close()

        # If the above code causes an exception, catch it, sleep, and then retry the query if allowed
        except Exception as e:

            print(f"An exception has occurred:\n{e}")

            if retries_remaining > 0:
                print("Execution will halt for 5 minutes, then resume with the same query.\n")
                sleep(300)
            else:
                raise Exception(f"Ran out of retries on page {i}. Data could not be gathered. Quitting.")
        
        # If no exception is encountered in the "try" block, then the retry loop can be broken
        else:

            break
    
    print(f"{len(records)} records returned.")

    # Break the main for loop if the last page is returned (records returned is less than max. per page)
    if len(records) < 50:
        print("This is the final page. Data fetch is finished.")
        break

print("----------------------------------------")

# Close the file, as we are done writing to it
csv_file.close()

print("Data written successfully.\n")
