package alien.api.catalogue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.io.TransferDetails;
import alien.io.TransferUtils;
import alien.user.AliEnPrincipal;

/**
 * List transfers
 */
public class ListTransfer extends Request {
	/**
	 *
	 */
	private static final long serialVersionUID = -4583213128245636923L;
	private List<TransferDetails> transfers;
	private final String status;
	private final String toSE;
	private final String user;
	private final Long id;
	private final int count;
	private final boolean sort_desc;

	/**
	 * @param user
	 * @param toSE
	 * @param userTransfer
	 * @param status
	 * @param id
	 * @param count
	 * @param desc
	 */
	public ListTransfer(final AliEnPrincipal user, final String toSE, final String userTransfer, final String status, final Long id, final int count, final boolean desc) {
		setRequestUser(user);
		this.status = status;
		this.toSE = toSE;
		this.user = userTransfer;
		this.id = id;
		this.count = count;
		this.sort_desc = desc;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.status, this.toSE, this.user, String.valueOf(this.id), String.valueOf(this.count), String.valueOf(this.sort_desc));
	}

	@Override
	public void run() {
		this.transfers = new ArrayList<>();
		if (this.count == 0)
			return;
		List<TransferDetails> tlst;

		tlst = TransferUtils.getAllActiveTransfers(this.toSE, this.user, this.status, this.id, (this.count == -1 ? null : Integer.valueOf(this.count)), this.sort_desc);

		for (final TransferDetails t : tlst)
			this.transfers.add(t);
	}

	/**
	 * @return transfer list
	 */
	public List<TransferDetails> getTransfers() {
		return this.transfers;
	}
}
