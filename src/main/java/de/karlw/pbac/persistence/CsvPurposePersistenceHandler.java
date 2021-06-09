package de.karlw.pbac.persistence;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import de.karlw.pbac.PurposeManager;
import de.karlw.pbac.PurposeSettings;
import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.purpose.PurposeSet;
import de.karlw.pbac.reservations.Reservation;
import de.karlw.pbac.subscriptions.SubscriptionAP;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class CsvPurposePersistenceHandler extends PurposePersistenceHandler {

    private static final @NotNull Logger log = LoggerFactory.getLogger(CsvPurposePersistenceHandler.class);

    String[] SAP_HEADERS = {"clientId", "topic", "ap", "qos"};
    String SAP_FILE_NAME = "subscriptionAPs.csv";

    String[] RES_HEADERS = {"topic", "aip_string"};
    String RES_FILE_NAME = "reservations.csv";

    @Override
    void saveSubscriptions(Collection<SubscriptionAP> saps) {
        FileWriter out = null;
        try {
            out = new FileWriter(SAP_FILE_NAME);
            CSVPrinter printer = new CSVPrinter(
                    out, CSVFormat.DEFAULT.withHeader(SAP_HEADERS));
            saps.forEach((sap) -> {
                try {
                    printer.printRecord(
                            sap.clientId,
                            sap.topic,
                            sap.ap.toString(),
                            String.valueOf(sap.qos)
                    );
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            out.close();
            log.debug("wrote {} saps to {}", saps.size(), SAP_FILE_NAME);

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Override
    void saveReservations(Collection<Reservation> reservations) {
        FileWriter out = null;
        try {
            out = new FileWriter(RES_FILE_NAME);
            CSVPrinter printer = new CSVPrinter(
                    out, CSVFormat.DEFAULT.withHeader(RES_HEADERS));
            reservations.forEach((res) -> {
                try {
                    if (res.aip == null) { return; }
                    printer.printRecord(
                            res.topic,
                            res.aip.toString()
                    );
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            out.close();
            log.debug("wrote {} reservations to {}", reservations.size(), SAP_FILE_NAME);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<Reservation> loadReservations() {

        List<Reservation> rs = new ArrayList<>();

        try {
            Reader in = new FileReader(RES_FILE_NAME);
            Iterable<CSVRecord> records = null;
            records = CSVFormat.DEFAULT
                    .withHeader(RES_HEADERS)
                    .withFirstRecordAsHeader()
                    .parse(in);
            for (CSVRecord record : records) {
                String topic = record.get("topic");
                PurposeSet aip = new PurposeSet(record.get("aip_string"));
                Reservation r = new Reservation(topic, aip);
                rs.add(r);
            }
        } catch (FileNotFoundException fe) {
            log.warn("no reservations file present");
            // will return empty list, which is intentional
        } catch (IOException e) {
            e.printStackTrace();
        }

        return rs;
    }

    public List<SubscriptionAP> loadSubscriptionAPs() {

        List<SubscriptionAP> rs = new ArrayList<>();

        try {
            Reader in = new FileReader(SAP_FILE_NAME);
            Iterable<CSVRecord> records = null;
            records = CSVFormat.DEFAULT
                    .withHeader(SAP_HEADERS)
                    .withFirstRecordAsHeader()
                    .parse(in);
            for (CSVRecord record : records) {
                String clientId = record.get("clientId");
                String topic = record.get("topic");
                Purpose ap = new Purpose(record.get("ap"));
                Integer qos = Integer.valueOf(record.get("qos"));
                SubscriptionAP r = new SubscriptionAP(clientId, topic, ap, qos);
                rs.add(r);
            }
        } catch (FileNotFoundException fe) {
            log.warn("no sap file present");
            // will return empty list, which is intentional
        } catch (IOException e) {
            e.printStackTrace();
        }

        return rs;
    }
}
