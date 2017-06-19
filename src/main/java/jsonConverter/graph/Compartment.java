package jsonConverter.graph;

public class Compartment {
	private final String m_strId;
	private final String m_strName;
	
	public Compartment(String id, String name) {
		// TODO Auto-generated constructor stub
		m_strId = id;
		m_strName = name;
	}

	public String getID() {
		return m_strId;
	}

	public String getName() {
		return m_strName;
	}
}
