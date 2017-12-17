package data.scraping.jsoup;

public class Researcher {

	private String idResearcher;
	private String name;
	private String phone;
	private String professionalSituation;
	private String orcid;
	private String researcherId;
	private String link;
	private String group;
	private String department;
	
	public Researcher(String idResearcher, String name, String phone, String professionalSituation, String orcid,
			String researcherId, String link, String group, String department) {
		super();
		this.idResearcher = idResearcher;
		this.name = name;
		this.phone = phone;
		this.professionalSituation = professionalSituation;
		this.orcid = orcid;
		this.researcherId = researcherId;
		this.link = link;
		this.group = group;
		this.department = department;
	}
	
	public Researcher() {
	
	}

	public String getIdResearcher() {
		return idResearcher;
	}

	public void setIdResearcher(String idResearcher) {
		this.idResearcher = idResearcher;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getProfessionalSituation() {
		return professionalSituation;
	}

	public void setProfessionalSituation(String professionalSituation) {
		this.professionalSituation = professionalSituation;
	}

	public String getOrcid() {
		return orcid;
	}

	public void setOrcid(String orcid) {
		this.orcid = orcid;
	}

	public String getResearcherId() {
		return researcherId;
	}

	public void setResearcherId(String researcherId) {
		this.researcherId = researcherId;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getDepartment() {
		return department;
	}

	public void setDepartment(String department) {
		this.department = department;
	}

	@Override
	public String toString() {
		return "Researcher [idResearcher=" + idResearcher + ", name=" + name + ", phone=" + phone
				+ ", professionalSituation=" + professionalSituation + ", orcid=" + orcid + ", researcherId="
				+ researcherId + ", link=" + link + ", group=" + group + ", department=" + department + "]";
	}
	
}
