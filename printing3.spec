%global gomodulesmode GO111MODULE=auto
%define debug_package %{nil}

Name:		printing3
Version:	0.0.1
Release:	1%{?dist}
Summary:	Source code printing pipeline for contests

License:	MIT
URL:		https://github.com/contester/printing3
Source0:	%{name}-%{version}.tar.gz

BuildRequires:	golang
BuildRequires:	systemd-rpm-macros
Requires(pre):	shadow-utils
%{?systemd_requires}

# busyprint: source -> pygmentize -> latex -> dvips -> PostScript.
# The list below is the set of packages that provide every TeX file the job
# template loads, plus the fonts dvips embeds (cm-super) and the metafont
# sources the Cyrillic typewriter font (latt) is generated from (lh, metafont).
Requires:	python3-pygments
Requires:	texlive-latex
Requires:	texlive-dvips
Requires:	texlive-dviinfox
Requires:	texlive-metafont
Requires:	texlive-amsmath
Requires:	texlive-amsfonts
Requires:	texlive-babel
Requires:	texlive-babel-english
Requires:	texlive-babel-russian
Requires:	texlive-bold-extra
Requires:	texlive-cyrillic
Requires:	texlive-fancyhdr
Requires:	texlive-fancyvrb
Requires:	texlive-graphics
Requires:	texlive-lastpage
Requires:	texlive-marvosym
Requires:	texlive-cm-super
Requires:	texlive-lh

%description
Print jobs from the contest web application travel through STOMP queues.
printing3-busyprint turns the source code into highlighted PostScript with
pygments and LaTeX. printagent, a Windows program shipped in
%{_datadir}/%{name}, receives the PostScript and sends it to the printer.

%pre
getent group %{name} >/dev/null || groupadd -r %{name}
getent passwd %{name} >/dev/null || \
    useradd -r -g %{name} -d %{_sharedstatedir}/%{name} -s /sbin/nologin \
    -c "printing3" %{name}
exit 0

%post
%systemd_post printing3-busyprint.service

%preun
%systemd_preun printing3-busyprint.service

%postun
%systemd_postun_with_restart printing3-busyprint.service

%prep
%setup -q

%build
%gobuild -o bin/printing3-busyprint github.com/contester/%{name}/busyprint
GOOS=windows GOARCH=amd64 go build -o bin/printagent-amd64.exe github.com/contester/%{name}/printagent

%install
%{__install} -v -D -t %{buildroot}%{_bindir} bin/printing3-busyprint
%{__install} -v -D -m 644 -t %{buildroot}%{_unitdir} printing3-busyprint.service
%{__install} -v -D -m 600 -T printing3-busyprint.sysconfig %{buildroot}%{_sysconfdir}/sysconfig/printing3-busyprint
%{__install} -v -D -m 644 -t %{buildroot}%{_datadir}/%{name} bin/printagent-amd64.exe printagent.example.toml

%files
%{_bindir}/printing3-busyprint
%{_unitdir}/printing3-busyprint.service
%config(noreplace) %{_sysconfdir}/sysconfig/printing3-busyprint
%dir %{_datadir}/%{name}
%{_datadir}/%{name}/*

%changelog
* Sun Nov 01 2026 Paul Komkoff <i@stingr.net> 0.0.1-1
- initial package: busyprint service and cross-built printagent for Windows
