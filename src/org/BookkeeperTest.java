package org;

import java.io.IOException;
import java.util.Enumeration;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.KeeperException;

public class BookkeeperTest {

	public static void main(String[] args) throws IOException,
			InterruptedException, KeeperException, BKException {
		ClientConfiguration conf = new ClientConfiguration();
		conf.setZkServers("bookeeper01:2181,bookeeper02:2181,bookeeper03:2181");

		BookKeeper client = new BookKeeper(conf);

		LedgerHandle lh = client.createLedger(3, 2, DigestType.CRC32,
				"foobar".getBytes());

		lh.addEntry("Hello World1!".getBytes());
		lh.addEntry("Hello World2!".getBytes());
		lh.addEntry("Hello World3!".getBytes());
		lh.addEntry("Hello World4!".getBytes());
		lh.addEntry("Hello World5!".getBytes());
		lh.addEntry("Hello World6!".getBytes());
		lh.close();

		LedgerHandle lh2 = client.openLedger(lh.getId(), DigestType.CRC32,
				"foobar".getBytes());
		System.out.println(lh.getId());
		System.out.println(lh2.getId());
		long lastEntry = lh2.getLastAddConfirmed();
		System.out.println(lastEntry);
		Enumeration<LedgerEntry> entries = lh2.readEntries(0, lastEntry);
		while (entries.hasMoreElements()) {
			byte[] bytes = entries.nextElement().getEntry();
			System.out.println(new String(bytes));
		}
		client.deleteLedger(lh2.getId());
		System.exit(0);

	}

}
