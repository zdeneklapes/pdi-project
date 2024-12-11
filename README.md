# README.md

## Project PDI - Process Real-Time Public Transit Data Using Apache Spark/Flink

### Author: Zdenek Lapes, xlapes02

---

## Overview

Installation according to [INSTALL.md](INSTALL.md) and testing according to [TESTING.md](TESTING.md)

## References

- https://rychly-edu.gitlab.io/dist-apps/project/#zpracovani-proudu-dat-arcgis-stream-services-z-dopravy-idsjmk-pomoci-apache-sparkflink
- https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/overview/
- https://nightlies.apache.org/flink/flink-docs-master/api/python/reference/

## [Assignment](https://rychly-edu.gitlab.io/dist-apps/project/#zpracovani-proudu-dat-arcgis-stream-services-z-dopravy-idsjmk-pomoci-apache-sparkflink)



### Data Source:

- "Public Transit Vehicle Positions" from the Open Data Portal of Statutory City of Brno.
- Published via ArcGIS Stream Services.

### Objectives:

1. **Real-Time Data Retrieval**
    - Utilize WebSocket for data retrieval.
    - Example (using cURL/OpenSSL):
      ```
      curl -H 'Upgrade: websocket' \
           -H "Sec-WebSocket-Key: $(openssl rand -base64 16)" \
           -H 'Sec-WebSocket-Version: 13' \
           --http1.1 -sSv \
           'https://gis.brno.cz/geoevent/ws/services/ODAE_public_transit_stream/StreamServer/subscribe?outSR=4326'
      ```

2. **Develop an Application with Apache Spark or Flink**
   The application must connect to the data source and perform the following tasks for active vehicles (`isinactive = false`):

    - **Task 1: Filter Vehicles in a Rectangular Area**
        - Utilize attributes `lat` and `lon`.
        - Check if the values fall within a specified range.

    - **Task 2: List Trolleybuses at Final Stops**
        - Identify trolleybuses where `laststopid == finalstopid`.
        - For each trolleybus, output:
            - Stop ID or name.
            - Timestamp of the last update.

    - **Task 3: List Delayed Vehicles Reducing Delay**
        - Identify vehicles that reduced their delay (`delay`) in the last report.
        - Sort results in descending order based on the difference between the last two delay values.
        - The vehicle with the greatest improvement is listed first.

    - **Task 4: Min/Max Delay Over the Last 3 Minutes**
        - Calculate the minimum and maximum delay from all delayed vehicles reported in the last 3 minutes.

    - **Task 5: Min/Max Interval Between Reports**
        - Consider the last 10 reports for each vehicle.
        - Calculate and output the minimum and maximum time intervals between consecutive reports.

---

### Additional Requirements:

- Use any programming language supported by the chosen platform and any library (except complete solutions of similar work).
- Cite all used sources in the project documentation (`README.md`) including their licenses.
- CLI/TUI output is sufficient; no GUI required.
- Allow evaluation of tasks either concurrently or by selecting a specific task during application startup.
- The solution should run on a single machine (with embedded Spark/Flink) and be deployable on a cluster.
- Include instructions for local execution and cluster deployment in `INSTALL.md`.
- Provide tests to verify functionality using test data (`TESTING.md`).
- Deliver the solution as a ZIP or TAR+GZip archive containing the project repository (excluding `.git`).

### Deliverables:

- Implementation of the application.
- Documentation:
    - `README.md` (sources and licenses).
    - `INSTALL.md` (setup and deployment).
    - `TESTING.md` (tests and expected results).
