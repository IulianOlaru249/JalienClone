package alien.api.catalogue;

import java.util.List;

import alien.api.Request;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;
import utils.ExpireTime;

/**
 * @author ibrinzoi
 * @since 2021-01-19
 */
public class LFNExpireTime extends Request {
	private static final long serialVersionUID = -7311319846251895444L;

	private final List<String> paths;
	private final ExpireTime expireTime;
	private final boolean extend;

	/**
	 * @param user
	 * @param paths
	 * @param expireTime
	 * @param extend
	 */
	public LFNExpireTime(final AliEnPrincipal user, final List<String> paths, final ExpireTime expireTime, final boolean extend) {
		setRequestUser(user);
		this.paths = paths;
		this.expireTime = expireTime;
		this.extend = extend;
	}

	@Override
	public void run() {
		LFNUtils.setLFNExpireTime(getEffectiveRequester(), paths, expireTime, extend);
	}

	@Override
	public List<String> getArguments() {
		return paths;
	}

}